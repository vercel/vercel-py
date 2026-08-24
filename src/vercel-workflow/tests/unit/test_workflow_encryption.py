"""Reading the `encr` and `encp` payloads `@workflow/world-vercel` writes.

Every input the formats fix is checkable without a deployment: the HKDF
parameters that derive the key, the AES-GCM envelope around the payload, the
X25519 sealed box an outside writer puts a payload in, and what a missing or
wrong key is reported as.

Encrypting is done with `cryptography`'s own AES-GCM, HKDF and X25519, so what
is under test is this SDK's reading of the formats rather than a round trip
through one implementation. The `encp` labels, on which two implementations
agreeing is the whole game, are pinned additionally by frozen vectors taken
from `@workflow/core`'s own sealed box under Node's WebCrypto -- see
:data:`TS_SEALED`.
"""

from __future__ import annotations

import base64
import builtins
import os
import subprocess
import sys
import typing
from typing import Any

import httpx
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from vercel.oidc import VercelOidcTokenError
from vercel.workflow._internal import (
    encryption,
    py_sandbox,
    runtime,
    serialization as ser,
    world as w,
)
from vercel.workflow._internal.worlds import vercel as vercel_world
from vercel.workflow._internal.worlds.vercel import VercelWorld

from ..world_stubs import NoStreams

DEPLOYMENT_KEY = bytes(range(32))
PROJECT_ID = "prj_test"
RUN_ID = "wrun_test"

RUN_KEY = bytes.fromhex("e522c64592a43259bb4556108ddf2d6d45140f972598a512d59caf88a736f75f")
"""``derive_run_key(DEPLOYMENT_KEY, PROJECT_ID, RUN_ID)``, spelled out.

Frozen so the vectors below are reproducible from the file: they were generated
against these bytes, and a change to the symmetric derivation should fail
:func:`test_the_run_key_is_hkdf_over_project_then_run` rather than quietly
invalidate them.
"""

# Vectors from `@workflow/core`'s `sealed-box.ts`, run under Node's WebCrypto
# against RUN_KEY. Between them they pin everything a reimplementation can get
# wrong quietly: both HKDF labels, the zero salts, the order of the two public
# keys in the content key's `info`, and the absence of AAD.
TS_PUBLIC_KEY = bytes.fromhex("45414d375e4b6293fde5e196eb25ec0ec72bd7d34e374fbc7b2cc492be235f73")
TS_SEALED = bytes.fromhex(
    "c7fcdb12dbbe8bb7153833f61d39f4be9ad80280044fed28135f20d68f5e0e11"
    "28259a396106e81bbac16637e12a5cbba8ab4b948391e30548983c6472dba9e7"
    "9599d23d46c1ee7720c7f4b086d32f"
)
"""``seal(publicKey, 'devl[["amount"],21]')`` -- no AAD, which is what TS sends."""

TS_SEALED_WITH_AAD = bytes.fromhex(
    "d59040c072755a12b5be6804c8f33ef8ef4851f14f63cf454e91f2e21e932203"
    "089425b4b0718dacc2b782437cca67974aa136708011501ae80b5460ef77c8de"
    "cec7a860bcc41b231f78f2964d7246"
)
"""The same payload sealed under ``runAad(projectId, runId)``, which TS never does."""


def _oidc_token(token: str | None):
    """A stand-in for ``get_vercel_oidc_token``; ``None`` makes it fail.

    Patched rather than left to the environment: the real one reads a process
    cache and can try to refresh against a local `.vercel`, so what it answers
    would otherwise depend on the machine the tests run on.
    """

    async def get_vercel_oidc_token() -> str:
        if token is None:
            raise VercelOidcTokenError("no OIDC request header and no local project context")
        return token

    return get_vercel_oidc_token


def _no_cryptography(monkeypatch):
    """An ``__import__`` that refuses `cryptography`, and a cleared cache for it."""
    real_import = builtins.__import__
    for module in [name for name in sys.modules if name.startswith("cryptography")]:
        monkeypatch.delitem(sys.modules, module)

    def blocked(name, *args, **kwargs):
        if name.startswith("cryptography"):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    return blocked


def seal(key: bytes, plaintext: bytes, nonce: bytes = bytes(12)) -> bytes:
    """An `encr` payload, built the way the TypeScript side builds one."""
    return ser.ENCRYPTED + nonce + AESGCM(key).encrypt(nonce, plaintext, None)


def _hkdf(ikm: bytes, info: bytes) -> bytes:
    return HKDF(algorithm=SHA256(), length=32, salt=bytes(32), info=info).derive(ikm)


def seal_to(run_key: bytes, plaintext: bytes, *, aad: bytes | None = None) -> bytes:
    """An `encp` payload addressed to the run *run_key* belongs to.

    What an external hook resumer writes: it knows the recipient's public key
    and nothing else, so everything secret here is the fresh ephemeral keypair.
    """
    recipient = X25519PrivateKey.from_private_bytes(
        _hkdf(run_key, encryption.SCALAR_INFO)
    ).public_key()
    ephemeral = X25519PrivateKey.generate()
    ephemeral_public_key = ephemeral.public_key().public_bytes_raw()
    content_key = _hkdf(
        ephemeral.exchange(recipient),
        encryption.CONTENT_KEY_INFO + ephemeral_public_key + recipient.public_bytes_raw(),
    )
    nonce = os.urandom(12)
    return (
        ser.SEALED
        + ephemeral_public_key
        + nonce
        + AESGCM(content_key).encrypt(nonce, plaintext, aad)
    )


# ═══════════════════════════════════════════════════════════════════════════
# the key
# ═══════════════════════════════════════════════════════════════════════════


def test_hkdf_matches_rfc_5869() -> None:
    # RFC 5869 appendix A.1, the SHA-256 basic case.
    okm = encryption.hkdf_sha256(
        ikm=bytes.fromhex("0b" * 22),
        salt=bytes.fromhex("000102030405060708090a0b0c"),
        info=bytes.fromhex("f0f1f2f3f4f5f6f7f8f9"),
        length=42,
    )
    assert okm.hex() == (
        "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
    )


@pytest.mark.parametrize("length", [16, 32, 64])
def test_hkdf_matches_an_independent_implementation(length: int) -> None:
    # More than one output block, so the expand loop's chaining is covered too.
    expected = HKDF(algorithm=SHA256(), length=length, salt=bytes(32), info=b"info").derive(
        DEPLOYMENT_KEY
    )

    assert (
        encryption.hkdf_sha256(ikm=DEPLOYMENT_KEY, salt=bytes(32), info=b"info", length=length)
        == expected
    )


def test_the_run_key_is_hkdf_over_project_then_run() -> None:
    # The `info` string is `<projectId>|<runId>`: project first, a literal pipe,
    # no spaces. Every way of getting it wrong produces a well-formed key that
    # simply decrypts nothing.
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)

    assert key == HKDF(
        algorithm=SHA256(), length=32, salt=bytes(32), info=f"{PROJECT_ID}|{RUN_ID}".encode()
    ).derive(DEPLOYMENT_KEY)
    # And it is still the key the `encp` vectors were generated against.
    assert key == RUN_KEY


def test_the_salt_length_does_not_change_the_key() -> None:
    # `world-vercel` writes 32 zero bytes and cites RFC 5869 §3.1, which reads
    # as though the exact spelling matters. It does not: HMAC pads any key
    # shorter than its 64-byte block, so every zero salt up to that length -- and
    # the absent one -- extracts the same PRK. Recorded so that nobody spends a
    # debugging session on the salt, the way this file's first version did.
    keys = {
        encryption.hkdf_sha256(ikm=DEPLOYMENT_KEY, salt=salt, info=b"i", length=32)
        for salt in (b"", bytes(32), bytes(64))
    }

    over_a_block = encryption.hkdf_sha256(ikm=DEPLOYMENT_KEY, salt=bytes(65), info=b"i", length=32)

    assert len(keys) == 1
    assert over_a_block not in keys


def test_a_run_key_is_bound_to_its_run_and_project() -> None:
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    other_run = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id="wrun_2")
    other_project = encryption.derive_run_key(DEPLOYMENT_KEY, project_id="prj_2", run_id=RUN_ID)
    # `p|r` and `pr` would collide if the separator were dropped, and `a|b`
    # would equal `ab|` if the two halves were concatenated the other way.
    swapped = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=RUN_ID, run_id=PROJECT_ID)

    assert len({key, other_run, other_project, swapped}) == 4


def test_a_deployment_key_must_be_thirty_two_bytes() -> None:
    with pytest.raises(ValueError, match="32 bytes, got 16"):
        encryption.derive_run_key(bytes(16), project_id=PROJECT_ID, run_id=RUN_ID)


def test_a_base64_key_is_checked_before_it_is_used() -> None:
    assert encryption.decode_key(base64.b64encode(DEPLOYMENT_KEY).decode(), what="k") == (
        DEPLOYMENT_KEY
    )
    # A trailing newline in an environment variable, and the URL-safe alphabet
    # `Buffer.from(key, 'base64')` also accepts, are not what a decode error
    # should be spent on.
    urlsafe = base64.urlsafe_b64encode(bytes([251, 255]) + DEPLOYMENT_KEY[2:]).decode()
    assert encryption.decode_key(f" {urlsafe}\n", what="k")[:2] == b"\xfb\xff"
    with pytest.raises(ValueError, match="k is not valid base64"):
        encryption.decode_key("not base64!", what="k")
    with pytest.raises(ValueError, match="k decodes to 3 bytes"):
        encryption.decode_key(base64.b64encode(b"abc").decode(), what="k")


# ═══════════════════════════════════════════════════════════════════════════
# the envelope
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("size", [0, 1, 15, 16, 17, 1000])
def test_the_envelope_opens_what_webcrypto_sealed(size: int) -> None:
    # Block-boundary sizes: GCM's counter mode and its GHASH both special-case
    # a partial trailing block, in different ways.
    key = AESGCM.generate_key(bit_length=256)
    nonce, plaintext = os.urandom(12), os.urandom(size)
    sealed = AESGCM(key).encrypt(nonce, plaintext, None)

    assert encryption.open_envelope(key, nonce + sealed) == plaintext


def test_the_envelope_carries_no_additional_data() -> None:
    # `world-vercel` calls `aesGcmEncrypt(aesKey, data)` with `aad` omitted --
    # the AAD machinery in that module is for `encp`.
    key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)
    with_aad = AESGCM(key).encrypt(nonce, b"payload", b"aad")

    with pytest.raises(encryption.DecryptionError, match="authentication failed"):
        encryption.open_envelope(key, nonce + with_aad)


def test_a_tampered_payload_says_authentication_failed() -> None:
    # The alternative -- returning what the counter mode produced anyway --
    # surfaces later as a devalue parse error over binary garbage.
    key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)
    sealed = bytearray(AESGCM(key).encrypt(nonce, b"devl[42]", None))
    sealed[-1] ^= 0x01

    with pytest.raises(encryption.DecryptionError, match="authentication failed"):
        encryption.open_envelope(key, nonce + bytes(sealed))


def test_a_wrong_key_is_the_same_failure() -> None:
    nonce = os.urandom(12)
    sealed = AESGCM(AESGCM.generate_key(bit_length=256)).encrypt(nonce, b"devl[42]", None)

    with pytest.raises(encryption.DecryptionError, match="the run key is wrong"):
        encryption.open_envelope(AESGCM.generate_key(bit_length=256), nonce + sealed)


def test_a_truncated_envelope_is_rejected_before_the_cipher() -> None:
    key = AESGCM.generate_key(bit_length=256)
    with pytest.raises(encryption.DecryptionError, match="at least 28 bytes"):
        encryption.open_envelope(key, os.urandom(27))


def test_cryptography_is_probed_once_at_import(monkeypatch) -> None:
    # Not per call, because a run input is hydrated inside the sandbox, whose
    # context has a private `sys.modules` -- an import reached from there would
    # build a second copy of a native extension. Blocking the import and
    # decrypting anyway is what pins that the binding already happened.
    monkeypatch.setattr(builtins, "__import__", _no_cryptography(monkeypatch))
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)

    assert encryption.open_envelope(key, seal(key, b"devl[42]")[4:]) == b"devl[42]"


def test_decryption_works_inside_the_workflow_sandbox() -> None:
    # A run input is hydrated inside the sandbox, so this is the path that
    # `test_cryptography_is_probed_once_at_import` protects, driven end to end.
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    payload = seal(key, ser.dehydrate([{"amount": 21}]))

    with py_sandbox.Sandbox().enter():
        assert ser.hydrate(payload, what="the input of run wrun_1", key=key) == [{"amount": 21}]


def _first_call_of_a_fresh_interpreter(body: str, payload: bytes) -> str:
    """Run *body* with *payload* in ``sys.argv[1]``, in an interpreter of its own.

    The two tests below need the process to have made no cipher call at all
    before the one they make, and `cryptography` caches its deferred imports for
    the life of a process, so nothing a test does can put a warmed interpreter
    back. A fresh one per test is the only way to see the first call. It also
    means the payload has to be built out here and handed over as hex: sealing it
    in the child would be a cipher call, which is the thing being kept for later.
    """
    finished = subprocess.run(
        [sys.executable, "-c", body, payload.hex()],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert finished.returncode == 0, (
        f"exit {finished.returncode}\nstdout: {finished.stdout}\nstderr: {finished.stderr}"
    )
    return finished.stdout.strip()


_DECRYPT_IN_SANDBOX = """\
import sys
from vercel.workflow._internal import encryption, py_sandbox, serialization as ser

key = encryption.derive_run_key(bytes(range(32)), project_id="prj_test", run_id="wrun_test")
with py_sandbox.Sandbox().enter():
    print(ser.hydrate(bytes.fromhex(sys.argv[1]), what="the input of run wrun_1", key=key))
"""

_FAIL_TO_DECRYPT_IN_SANDBOX = """\
import sys
from vercel.workflow._internal import encryption, py_sandbox

key = encryption.derive_run_key(bytes(range(32)), project_id="prj_test", run_id="wrun_test")
with py_sandbox.Sandbox().enter():
    try:
        encryption.open_envelope(key, bytes.fromhex(sys.argv[1]))
    except encryption.DecryptionError as exc:
        print(exc)
"""


def test_the_first_decrypt_of_a_process_can_be_the_one_inside_the_sandbox() -> None:
    payload = seal(RUN_KEY, ser.dehydrate([{"amount": 21}]))

    assert _first_call_of_a_fresh_interpreter(_DECRYPT_IN_SANDBOX, payload) == "[{'amount': 21}]"


def test_the_first_failure_of_a_process_can_be_the_one_inside_the_sandbox() -> None:
    # Failing is its own path: raising `InvalidTag` is what imports
    # `cryptography.exceptions`, and every release defers that import to the
    # first call that needs it, on every Python. Reached inside the sandbox it
    # raises `SystemError` instead of reporting a bad payload, and aborts the
    # process outright on Python 3.14 -- so a run handed a payload it cannot
    # open has to be able to say so from here.
    payload = seal(AESGCM.generate_key(bit_length=256), b"devl[42]")

    said = _first_call_of_a_fresh_interpreter(_FAIL_TO_DECRYPT_IN_SANDBOX, payload[4:])
    assert "authentication failed" in said


# ═══════════════════════════════════════════════════════════════════════════
# the sealed box
# ═══════════════════════════════════════════════════════════════════════════


def test_a_run_keypair_is_hkdf_over_the_run_key() -> None:
    # The labelled derivation is what keeps the scalar independent of every
    # other use of the same 32 bytes -- the AES key among them.
    scalar, public_key = encryption.derive_run_key_pair(RUN_KEY)

    assert scalar == _hkdf(RUN_KEY, b"workflow/encp/x25519/v1")
    assert public_key == X25519PrivateKey.from_private_bytes(scalar).public_key().public_bytes_raw()


def test_the_keypair_matches_the_one_typescript_derives() -> None:
    # The public key is what a resumer seals to, so a Python reader deriving a
    # different one cannot open anything -- and the failure would surface only
    # as an authentication error, naming nothing.
    assert encryption.derive_run_key_pair(RUN_KEY).public_key == TS_PUBLIC_KEY


def test_a_keypair_is_bound_to_its_run_key() -> None:
    other = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id="wrun_2")

    assert encryption.derive_run_key_pair(other).public_key != TS_PUBLIC_KEY


def test_the_der_prefixes_are_the_headers_cryptography_itself_writes() -> None:
    # Both prefixes are hand-assembled hex that every other test reaches only
    # through a key operation, so a wrong byte gets diagnosed somewhere else:
    # `exchange` catches the DER failure as a refused point, and the caller
    # reports it as the sealed payload's ephemeral key being invalid -- blaming
    # the incoming payload, which is fine. Comparing against the library's own
    # encoding of the same key names the constant instead.
    private = X25519PrivateKey.generate()
    pkcs8 = private.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
    spki = private.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

    assert pkcs8 == encryption.PKCS8_X25519_PREFIX + private.private_bytes_raw()
    assert spki == encryption.SPKI_X25519_PREFIX + private.public_key().public_bytes_raw()


def test_a_run_key_must_be_thirty_two_bytes_to_derive_a_keypair() -> None:
    with pytest.raises(ValueError, match="32 bytes, got 16"):
        encryption.derive_run_key_pair(bytes(16))


@pytest.mark.parametrize("size", [0, 1, 15, 16, 17, 1000])
def test_the_sealed_box_opens_what_an_outside_writer_sealed(size: int) -> None:
    plaintext = os.urandom(size)

    payload = seal_to(RUN_KEY, plaintext)

    assert encryption.open_sealed_envelope(RUN_KEY, payload[4:]) == plaintext


def test_the_sealed_box_opens_what_typescript_sealed() -> None:
    # The end of the argument: a payload this SDK never produced, from the
    # implementation that will be on the other side of every real hook resume.
    assert encryption.open_sealed_envelope(RUN_KEY, TS_SEALED) == b'devl[["amount"],21]'


def test_the_sealed_box_carries_no_additional_data() -> None:
    # `resumeHook` seals with `sealTo(runPublicKey)` and passes no AAD; the
    # construction binds the payload to its recipient through the content key's
    # `info` instead. Reading one *with* AAD would mean we had guessed at an
    # AAD the writer does not send, and every real payload would fail.
    with pytest.raises(encryption.DecryptionError, match="authentication failed"):
        encryption.open_sealed_envelope(RUN_KEY, TS_SEALED_WITH_AAD)


def test_a_payload_sealed_to_another_run_does_not_open() -> None:
    # The content key covers both public keys, so a payload replayed at a
    # different run fails to authenticate rather than opening under the wrong
    # identity.
    other = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id="wrun_2")

    with pytest.raises(encryption.DecryptionError, match="authentication failed"):
        encryption.open_sealed_envelope(other, TS_SEALED)


def test_a_tampered_sealed_payload_says_authentication_failed() -> None:
    payload = bytearray(seal_to(RUN_KEY, b"devl[42]")[4:])
    payload[-1] ^= 0x01

    with pytest.raises(encryption.DecryptionError, match="authentication failed"):
        encryption.open_sealed_envelope(RUN_KEY, bytes(payload))


def test_a_truncated_sealed_payload_is_rejected_before_the_cipher() -> None:
    # 32 bytes of ephemeral public key, then the 28 an AES-GCM envelope needs.
    with pytest.raises(encryption.DecryptionError, match="at least 60 bytes"):
        encryption.open_sealed_envelope(RUN_KEY, os.urandom(59))


def test_an_unusable_ephemeral_key_is_named_rather_than_authenticated() -> None:
    # A low-order point agrees to an all-zero secret. Deriving a content key
    # from it anyway would report an authentication failure and send the reader
    # after the key material instead of the payload.
    payload = bytes(32) + os.urandom(28)

    with pytest.raises(encryption.DecryptionError, match="not a valid curve point"):
        encryption.open_sealed_envelope(RUN_KEY, payload)


def test_x25519_is_bound_once_at_import(monkeypatch) -> None:
    # As with AES-GCM, and the reason the DER loaders are the entry point rather
    # than `X25519PrivateKey.from_private_bytes`: a sealed hook payload is opened
    # inside the sandbox, where an import would re-execute the native extension.
    # Blocking every `cryptography` import and opening one anyway is what pins
    # that no call reaches for one.
    payload = seal_to(RUN_KEY, b"devl[42]")
    monkeypatch.setattr(builtins, "__import__", _no_cryptography(monkeypatch))

    assert encryption.open_sealed_envelope(RUN_KEY, payload[4:]) == b"devl[42]"


def test_opening_a_sealed_payload_works_inside_the_workflow_sandbox() -> None:
    payload = seal_to(RUN_KEY, ser.dehydrate({"type": "approve"}))

    with py_sandbox.Sandbox().enter():
        assert ser.hydrate(payload, what="the payload of hook hook_1", key=RUN_KEY) == {
            "type": "approve"
        }


# ═══════════════════════════════════════════════════════════════════════════
# the payload
# ═══════════════════════════════════════════════════════════════════════════


def test_an_encrypted_payload_hydrates_through_its_inner_prefix() -> None:
    # The plaintext's own prefix is dispatched like any other payload, which is
    # what would make a future `gzip`-inside-`encr` work without touching this.
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    payload = seal(key, ser.dehydrate([{"amount": 21}]))

    assert ser.hydrate(payload, what="the input of run wrun_1", key=key) == [{"amount": 21}]


def test_a_decrypted_payload_that_is_not_a_payload_is_reported() -> None:
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    payload = seal(key, b"json{}")

    with pytest.raises(ser.SerializationError, match="unknown serialization format"):
        ser.hydrate(payload, what="the input of run wrun_1", key=key)


def test_a_sealed_payload_hydrates_through_its_inner_prefix() -> None:
    # The shape of every hook resume on the Vercel world: the payload comes
    # from outside the run, so it arrives sealed rather than encrypted.
    payload = seal_to(RUN_KEY, ser.dehydrate({"type": "approve", "id": "1"}))

    assert ser.hydrate(payload, what="the payload of hook hook_1", key=RUN_KEY) == {
        "type": "approve",
        "id": "1",
    }


def test_an_encrypted_payload_without_a_key_names_the_problem() -> None:
    with pytest.raises(ser.SerializationError, match="the input of run wrun_1 is encrypted"):
        ser.hydrate(ser.ENCRYPTED + bytes(28), what="the input of run wrun_1")


def test_a_sealed_payload_without_a_key_says_which_way_it_is_encrypted() -> None:
    # Distinguished from `encr` because the two are resolved differently on the
    # writing side: nobody wrote this run's own key, so "the run that wrote it"
    # would point at the wrong run.
    with pytest.raises(ser.SerializationError, match="the payload of hook hook_1 is sealed"):
        ser.hydrate(ser.SEALED + bytes(56), what="the payload of hook hook_1")


def test_a_sealed_payload_that_does_not_authenticate_names_the_payload() -> None:
    with pytest.raises(ser.SerializationError, match="Cannot decrypt the payload of hook hook_1"):
        ser.hydrate(ser.SEALED + TS_SEALED, what="the payload of hook hook_1", key=bytes(32))


def test_a_payload_that_does_not_authenticate_names_the_payload() -> None:
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    payload = seal(key, ser.dehydrate(42))

    with pytest.raises(ser.SerializationError, match="Cannot decrypt the input of run wrun_1"):
        ser.hydrate(payload, what="the input of run wrun_1", key=bytes(32))


def test_only_an_encrypted_payload_asks_for_a_key() -> None:
    # What keeps a plaintext run off the resolution path, and with it the API
    # request that resolving can cost. A sealed payload asks for the same 32
    # bytes -- its keypair derives from them -- so it belongs on that path too.
    assert ser.is_encrypted(ser.ENCRYPTED + bytes(28))
    assert ser.is_encrypted(ser.SEALED + bytes(60))
    assert not ser.is_encrypted(ser.dehydrate(42))
    # A run's `input` is `bytes | str`: `run_created` echoes back '[Circular]'.
    assert not ser.is_encrypted("[Circular]")
    assert not ser.is_encrypted(None)


# ═══════════════════════════════════════════════════════════════════════════
# resolving the key on the Vercel world
# ═══════════════════════════════════════════════════════════════════════════


def _deployed(monkeypatch, **overrides: str | None) -> None:
    """The environment of a function running on a Vercel deployment."""
    env = {
        "VERCEL": "1",
        "VERCEL_DEPLOYMENT_ID": "dpl_current",
        "VERCEL_PROJECT_ID": PROJECT_ID,
        "VERCEL_DEPLOYMENT_KEY": base64.b64encode(DEPLOYMENT_KEY).decode(),
        **overrides,
    }
    for name, value in env.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)


async def test_a_run_of_this_deployment_derives_its_key_in_process(monkeypatch) -> None:
    # No network: HKDF over the deployment secret is the whole resolution.
    _deployed(monkeypatch)
    world = VercelWorld()

    key = await world.run_key(RUN_ID)

    assert key == encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    assert await world.run_key(RUN_ID, deployment_id="dpl_current") == key


async def test_local_derivation_needs_a_project_id(monkeypatch) -> None:
    # Getting it wrong yields a GCM failure rather than anything that names it.
    _deployed(monkeypatch, VERCEL_PROJECT_ID=None)
    world = VercelWorld()

    with pytest.raises(RuntimeError, match="no project id"):
        await world.run_key(RUN_ID)


@respx.mock
async def test_a_run_of_another_deployment_asks_the_api(monkeypatch) -> None:
    # The secret in this process derives keys for this deployment's runs only.
    _deployed(monkeypatch)
    expected = os.urandom(32)
    route = respx.get(
        "https://api.vercel.com/v1/workflow/run-key/dpl_other",
    ).mock(return_value=httpx.Response(200, json={"key": base64.b64encode(expected).decode()}))
    world = VercelWorld(token="test-token")

    assert await world.run_key(RUN_ID, deployment_id="dpl_other") == expected

    request = route.calls.last.request
    assert dict(request.url.params) == {"projectId": PROJECT_ID, "runId": RUN_ID}
    assert request.headers["Authorization"] == "Bearer test-token"
    # `fetchRunKey` sends `Authorization` and nothing else. The world's own
    # headers steer the workflow-server proxy and have no meaning here.
    assert "x-vercel-project-id" not in request.headers
    assert "x-vercel-workflow-api-url" not in request.headers


@respx.mock
async def test_the_api_token_is_config_then_env_then_oidc(monkeypatch) -> None:
    # `resolveVercelApiToken`'s order. `VERCEL_TOKEN` is the rung for tooling
    # with no OIDC token to fetch -- the CLI, and the e2e driver that creates
    # these runs -- so it has to win over OIDC, not merely exist.
    _deployed(monkeypatch)
    monkeypatch.setattr(vercel_world, "get_vercel_oidc_token", _oidc_token("oidc_tok"))
    route = respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_other").mock(
        return_value=httpx.Response(200, json={"key": None})
    )

    async def authorization(world: VercelWorld) -> str:
        await world.run_key(RUN_ID, deployment_id="dpl_other")
        return route.calls.last.request.headers["Authorization"]

    assert await authorization(VercelWorld()) == "Bearer oidc_tok"
    monkeypatch.setenv("VERCEL_TOKEN", "vt_from_env")
    assert await authorization(VercelWorld()) == "Bearer vt_from_env"
    assert await authorization(VercelWorld(token="cfg_tok")) == "Bearer cfg_tok"


@respx.mock
async def test_no_token_at_all_is_reported_before_the_request(monkeypatch) -> None:
    # Sending an unauthenticated request instead would come back as a 403 that
    # reads like a permissions problem.
    _deployed(monkeypatch, VERCEL_TOKEN=None)
    monkeypatch.setattr(vercel_world, "get_vercel_oidc_token", _oidc_token(None))
    route = respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_other")

    with pytest.raises(RuntimeError, match="no OIDC token or VERCEL_TOKEN"):
        await VercelWorld().run_key(RUN_ID, deployment_id="dpl_other")
    assert not route.called


@respx.mock
async def test_an_unexpected_oidc_failure_is_not_reported_as_a_missing_token(
    monkeypatch,
) -> None:
    # Only `VercelOidcTokenError` means "there is no token here". A broken
    # socket, or a bug in the token code, would otherwise be reported as an
    # unset environment variable and send the reader after the wrong thing.
    _deployed(monkeypatch, VERCEL_TOKEN=None)

    async def broken() -> str:
        raise httpx.ConnectError("the OIDC endpoint is unreachable")

    monkeypatch.setattr(vercel_world, "get_vercel_oidc_token", broken)

    with pytest.raises(httpx.ConnectError):
        await VercelWorld().run_key(RUN_ID, deployment_id="dpl_other")


@respx.mock
async def test_a_response_without_a_key_field_is_not_read_as_plaintext(monkeypatch) -> None:
    # `.get("key")` would turn a changed or truncated body into "this run is
    # not encrypted", and the `encr` payload would fail several frames later.
    _deployed(monkeypatch)
    respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_other").mock(
        return_value=httpx.Response(200, json={"unexpected": True})
    )

    with pytest.raises(w.WorkflowWorldError, match="Invalid response from the Vercel API"):
        await VercelWorld(token="t").run_key(RUN_ID, deployment_id="dpl_other")


@respx.mock
async def test_the_team_comes_from_the_world_not_the_environment(monkeypatch) -> None:
    # `world-vercel` passes `config.projectConfig.teamId` through with no
    # environment fallback -- unlike the project, which has one for the runtime.
    _deployed(monkeypatch)
    monkeypatch.setenv("VERCEL_TEAM_ID", "team_from_env")
    route = respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_other").mock(
        return_value=httpx.Response(200, json={"key": None})
    )

    await VercelWorld(token="t").run_key(RUN_ID, deployment_id="dpl_other")
    assert "teamId" not in route.calls.last.request.url.params

    await VercelWorld(token="t", project_id="prj_cfg", team_id="team_cfg").run_key(
        RUN_ID, deployment_id="dpl_other"
    )
    params = dict(route.calls.last.request.url.params)
    # The configured project wins over the environment on the same path.
    assert params == {"projectId": "prj_cfg", "runId": RUN_ID, "teamId": "team_cfg"}


@respx.mock
async def test_a_deployment_without_a_secret_reports_no_key(monkeypatch) -> None:
    # What `world-vercel` answers, rather than reaching for the API: a run of
    # this deployment is derived from the secret or not encrypted at all. If
    # the platform turns out not to inject one into a Python function, the
    # `encr` payload that follows says so instead of being papered over here.
    _deployed(monkeypatch, VERCEL_DEPLOYMENT_KEY=None)
    route = respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_current")

    assert await VercelWorld(token="test-token").run_key(RUN_ID) is None
    assert not route.called


@respx.mock
async def test_a_null_key_means_the_run_is_not_encrypted(monkeypatch) -> None:
    # Not an error: encryption is disabled for the run, and its payloads are
    # plaintext.
    _deployed(monkeypatch)
    respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_other").mock(
        return_value=httpx.Response(200, json={"key": None})
    )
    world = VercelWorld(token="test-token")

    assert await world.run_key(RUN_ID, deployment_id="dpl_other") is None


@respx.mock
async def test_a_failed_key_fetch_says_which_run(monkeypatch) -> None:
    # Outside a serverless function, where even a run of the deployment named
    # in the environment has to be fetched.
    _deployed(monkeypatch, VERCEL=None)
    respx.get("https://api.vercel.com/v1/workflow/run-key/dpl_current").mock(
        return_value=httpx.Response(403)
    )
    world = VercelWorld(token="test-token")

    with pytest.raises(w.WorkflowWorldError, match=f"key for run {RUN_ID}"):
        await world.run_key(RUN_ID)


async def test_a_world_that_does_not_encrypt_resolves_no_key() -> None:
    from vercel.workflow._internal.worlds.local import LocalWorld

    assert await LocalWorld().run_key(RUN_ID) is None


# ═══════════════════════════════════════════════════════════════════════════
# what the runtime asks for
# ═══════════════════════════════════════════════════════════════════════════


def _run(**overrides) -> w.WorkflowRun:
    return w.WorkflowRunAdaptor.from_wire(
        {
            "runId": RUN_ID,
            "status": "running",
            "deploymentId": "dpl_current",
            "workflowName": "wf",
            "createdAt": "2026-07-30T00:00:00.000Z",
            "updatedAt": "2026-07-30T00:00:00.000Z",
            "startedAt": "2026-07-30T00:00:00.000Z",
            **overrides,
        }
    )


class _CountingWorld(NoStreams, w.World):
    """A world that records how often the runtime asked it for a key."""

    def __init__(self, key: bytes | None) -> None:
        self._key = key
        self.asks = 0

    async def run_key(self, run_id: str, *, deployment_id: str | None = None) -> bytes | None:
        self.asks += 1
        return self._key

    async def get_deployment_id(self) -> str:
        raise NotImplementedError

    async def queue(self, queue_name, message, **kwargs) -> str:
        raise NotImplementedError

    def create_queue_handler(self, queue_name_prefix, handler):
        raise NotImplementedError

    async def runs_get(self, run_id: str):
        raise NotImplementedError

    async def steps_get(self, run_id: str, step_id: str):
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str):
        raise NotImplementedError

    async def events_create(self, run_id, data):
        raise NotImplementedError

    async def events_list(self, run_id, *, pagination=None):
        raise NotImplementedError


def test_every_serialized_event_field_is_reported_by_payloads() -> None:
    """``Event.payloads()`` must not miss a field that can hold a payload.

    One that it misses reads as "this run is not encrypted", and the run then
    fails on the payload it could not get a key for -- so the check is against
    the field annotations rather than against a list kept by hand here.
    """
    events = typing.get_args(typing.get_args(w.Event)[0])
    checked = 0

    for event_cls in events:
        data_field = event_cls.model_fields.get("event_data")
        if data_field is None:
            assert event_cls.model_construct().payloads() == ()
            continue
        # `bytes` for a payload field, `Any` for the error ones the TypeScript
        # side also encrypts. A plain `str`/`datetime`/`int` field is neither.
        data_cls = next(
            arg
            for arg in (typing.get_args(data_field.annotation) or (data_field.annotation,))
            if arg is not type(None)
        )
        payload_fields = {
            name
            for name, field in data_cls.model_fields.items()
            if field.annotation is Any or "bytes" in str(field.annotation)
        }
        if not payload_fields:
            continue
        sentinels = {name: f"payload of {name}".encode() for name in payload_fields}
        event = event_cls.model_construct(event_data=data_cls.model_construct(**sentinels))
        reported = event.payloads()

        omitted = sorted(name for name, value in sentinels.items() if value not in reported)
        assert not omitted, f"{event_cls.__name__}.payloads() omits {omitted}"
        checked += 1

    assert checked, "no event carried a payload field -- the reflection above broke"


async def test_a_plaintext_run_never_resolves_a_key() -> None:
    # No HKDF and no API request for a run that has nothing to decrypt -- and
    # so none of the ways resolving a key can fail, either.
    world = _CountingWorld(bytes(32))
    run = _run(input=ser.dehydrate([]))
    events: list[w.Event] = [
        w.StepCreatedEventData(
            step_name="pay", input=ser.dehydrate(ser.step_arguments((), {}))
        ).into_event("step_0"),
        w.StepCompletedEventData(result=ser.dehydrate(42)).into_event("step_0"),
        w.StepStartedEvent(correlation_id="step_0"),
        w.RunStartedEvent(),
    ]

    assert await runtime._resolve_run_key(world, run, events) is None
    assert world.asks == 0


@pytest.mark.parametrize(
    "where", ["input", "step result", "hook payload", "step input", "hook metadata"]
)
async def test_one_encrypted_payload_anywhere_resolves_the_key_once(where: str) -> None:
    # Any one of them may be the encrypted one on its own, and the scan covers
    # every payload field rather than the narrower set a replay hydrates: a
    # `step input` or `hook metadata` that needed a key and did not get one
    # would stand the run, and only ever an encrypted one.
    key = bytes(range(32))
    world = _CountingWorld(key)
    sealed = seal(key, ser.dehydrate(42))

    def payload(field: str, plain: Any) -> Any:
        return sealed if where == field else plain

    run = _run(input=payload("input", ser.dehydrate([])))
    events: list[w.Event] = [
        w.StepCreatedEventData(
            step_name="pay", input=payload("step input", ser.dehydrate({"args": []}))
        ).into_event("step_0"),
        w.StepCompletedEventData(result=payload("step result", ser.dehydrate(42))).into_event(
            "step_0"
        ),
        w.HookCreatedEventData(
            token="tok", metadata=payload("hook metadata", ser.dehydrate({}))
        ).into_event("hook_0"),
        w.HookReceivedEventData(payload=payload("hook payload", ser.dehydrate({}))).into_event(
            "hook_0"
        ),
    ]

    assert await runtime._resolve_run_key(world, run, events) == key
    assert world.asks == 1


async def test_a_sealed_hook_payload_resolves_the_key() -> None:
    # Gap 18 from the runtime's side, and the reason the scan cannot look for
    # `encr` alone: a run whose own payloads are plaintext still has to read a
    # hook payload an outside resumer sealed to it, and a scan that skipped
    # `encp` would answer "this run is not encrypted" and then fail the run.
    world = _CountingWorld(RUN_KEY)
    run = _run(input=ser.dehydrate([]))
    payload = seal_to(RUN_KEY, ser.dehydrate({"type": "approve"}))
    events: list[w.Event] = [
        w.HookCreatedEventData(token="tok").into_event("hook_0"),
        w.HookReceivedEventData(payload=payload).into_event("hook_0"),
    ]

    key = await runtime._resolve_run_key(world, run, events)

    assert key == RUN_KEY
    assert world.asks == 1
    assert ser.hydrate(payload, what="the payload of hook hook_0", key=key) == {"type": "approve"}
