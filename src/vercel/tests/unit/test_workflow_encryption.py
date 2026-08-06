"""Reading the `encr` payloads `@workflow/world-vercel` writes.

Every input the format fixes is checkable without a deployment: the HKDF
parameters that derive the key, the AES-GCM envelope around the payload, and
what a missing or wrong key is reported as.

Sealing is done with `cryptography`'s own AES-GCM and HKDF, so what is under
test is this SDK's reading of the format rather than a round trip through one
implementation.
"""

from __future__ import annotations

import base64
import builtins
import os
import sys
import typing
from typing import Any

import httpx
import pytest
import respx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from vercel._internal.workflow import (
    encryption,
    py_sandbox,
    runtime,
    serialization as ser,
    world as w,
)
from vercel._internal.workflow.worlds import vercel as vercel_world
from vercel._internal.workflow.worlds.vercel import VercelWorld
from vercel.oidc import VercelOidcTokenError
from vercel.tests.world_stubs import NoStreams

DEPLOYMENT_KEY = bytes(range(32))
PROJECT_ID = "prj_test"
RUN_ID = "wrun_test"


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
    assert encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID) == HKDF(
        algorithm=SHA256(), length=32, salt=bytes(32), info=f"{PROJECT_ID}|{RUN_ID}".encode()
    ).derive(DEPLOYMENT_KEY)


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


def test_without_the_extra_the_failure_names_the_remedy(monkeypatch) -> None:
    # `cryptography` is not in the default install, and a run on a deployment is
    # always encrypted -- so this is a plausible first encounter with the
    # feature, and "No module named 'cryptography'" would not say what to do.
    monkeypatch.setattr(encryption, "_AES_GCM_DECRYPT", None)

    with pytest.raises(encryption.DecryptionError, match=r'pip install "vercel\[encryption\]"'):
        encryption.open_envelope(bytes(32), os.urandom(28))


def test_the_extra_is_probed_once_at_import(monkeypatch) -> None:
    # Not per call, because a run input is hydrated inside the sandbox, whose
    # context has a private `sys.modules` -- an import reached from there would
    # build a second copy of a native extension. Blocking the import and
    # decrypting anyway is what pins that the binding already happened.
    monkeypatch.setattr(builtins, "__import__", _no_cryptography(monkeypatch))
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)

    assert encryption.open_envelope(key, seal(key, b"devl[42]")[4:]) == b"devl[42]"


def test_decryption_works_inside_the_workflow_sandbox() -> None:
    # A run input is hydrated inside the sandbox, so this is the path that
    # `test_the_extra_is_probed_once_at_import` protects, driven end to end.
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    payload = seal(key, ser.dehydrate([{"amount": 21}]))

    with py_sandbox.workflow_sandbox():
        assert ser.hydrate(payload, what="the input of run wrun_1", key=key) == [{"amount": 21}]


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


def test_an_encrypted_payload_without_a_key_names_the_problem() -> None:
    with pytest.raises(ser.SerializationError, match="the input of run wrun_1 is encrypted"):
        ser.hydrate(ser.ENCRYPTED + bytes(28), what="the input of run wrun_1")


def test_a_payload_that_does_not_authenticate_names_the_payload() -> None:
    key = encryption.derive_run_key(DEPLOYMENT_KEY, project_id=PROJECT_ID, run_id=RUN_ID)
    payload = seal(key, ser.dehydrate(42))

    with pytest.raises(ser.SerializationError, match="Cannot decrypt the input of run wrun_1"):
        ser.hydrate(payload, what="the input of run wrun_1", key=bytes(32))


def test_only_an_encrypted_payload_asks_for_a_key() -> None:
    # What keeps a plaintext run off the resolution path, and with it the API
    # request that resolving can cost.
    assert ser.is_encrypted(ser.ENCRYPTED + bytes(28))
    assert not ser.is_encrypted(ser.dehydrate(42))
    assert not ser.is_encrypted(ser.SEALED + bytes(28))
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
    from vercel._internal.workflow.worlds.local import LocalWorld

    assert await LocalWorld().run_key(RUN_ID) is None


# ═══════════════════════════════════════════════════════════════════════════
# what the runtime asks for
# ═══════════════════════════════════════════════════════════════════════════


def _run(**overrides) -> w.WorkflowRun:
    return w.WorkflowRunAdaptor.validate_python(
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
            stepName="pay", input=ser.dehydrate(ser.step_arguments((), {}))
        ).into_event("step_0"),
        w.StepCompletedEventData(result=ser.dehydrate(42)).into_event("step_0"),
        w.StepStartedEvent(correlationId="step_0"),
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
            stepName="pay", input=payload("step input", ser.dehydrate({"args": []}))
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
