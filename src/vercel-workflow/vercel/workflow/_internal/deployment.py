"""Cross-deployment startup negotiation using the TypeScript health protocol."""

from __future__ import annotations

import contextlib
import json
import logging
import time
import weakref
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import anyio
import semver

from . import encryption, serialization as ser, world as w
from .compression import CompressionMode

logger = logging.getLogger("vercel.workflow")
_PROBE_TIMEOUT = 10
_PROBE_RETRY_TIMEOUT = 2
_PROBE_CACHE_TTL = 600
_PROBE_MISS_TTL = 60
_PROBE_CACHE_MAX_ENTRIES = 256
_COMPRESSION_MIN_VERSION = semver.Version.parse("5.0.0-beta.18")

# A World scopes the deployments it can see. Run public keys are never cached.
_cache: weakref.WeakKeyDictionary[
    w.World, OrderedDict[tuple[str, str | None], tuple[float, dict[str, Any] | None]]
] = weakref.WeakKeyDictionary()
_warned = False


@dataclass(frozen=True)
class Target:
    deployment_id: str
    spec_version: int
    compression: CompressionMode
    execution_context: dict[str, Any]
    encryption_public_key: str | None = None

    async def encoder(
        self, world: w.World, run_id: str, *, compress_arguments: bool
    ) -> ser.PayloadEncoder:
        recipient = None
        if self.encryption_public_key is not None:
            try:
                recipient = encryption.decode_key(
                    self.encryption_public_key, what="the probed run public key"
                )
            except ValueError:
                pass
        return ser.PayloadEncoder(
            compression=self.compression
            if compress_arguments and self.spec_version >= w.SPEC_VERSION_SUPPORTS_COMPRESSION
            else "none",
            encryption_key=None
            if recipient is not None
            else await world.run_key(run_id, deployment_id=self.deployment_id),
            recipient_public_key=recipient,
        )


async def resolve(
    world: w.World, deployment_id: str | None, run_id: str, namespace: str | None
) -> Target:
    if deployment_id == "":
        raise ValueError("deployment_id must be a non-empty deployment ID or 'latest'")
    if deployment_id is None or deployment_id == "latest":
        current = await world.get_deployment_id()
        deployment_id = (
            await world.resolve_latest_deployment_id() if deployment_id == "latest" else current
        )
    else:
        try:
            current = await world.get_deployment_id()
        except Exception as error:
            logger.debug("Current deployment is unavailable; starting cross-deployment: %s", error)
            current = None
    if deployment_id == current:
        return Target(
            deployment_id,
            w.SPEC_VERSION_CURRENT,
            "auto",
            {"hookResumeInputVersion": w.HOOK_RESUME_INPUT_VERSION},
        )
    answer = await _probe(world, deployment_id, run_id, namespace)
    version = _spec_version(answer)
    if version < 2:
        raise RuntimeError("Starting on a deployment with legacy spec version 1 is not supported")
    context: dict[str, Any] = {}
    if answer:
        hook_version = answer.get("hookResumeInputVersion")
        if type(hook_version) in (int, float):
            context["hookResumeInputVersion"] = hook_version
        node_version = answer.get("nodeVersion")
        if isinstance(node_version, str):
            context["nodeVersion"] = node_version
    public_key = (answer or {}).get("encryptionPublicKey")
    return Target(
        deployment_id,
        version,
        _compression_mode(answer),
        context,
        public_key if isinstance(public_key, str) else None,
    )


def _parse_response(body: bytes) -> dict[str, Any] | None:
    if not body:
        return None
    try:
        parsed = json.loads(body.decode("utf-8", errors="replace"))
    except ValueError:
        return {"healthy": True, "format": "text"}
    if isinstance(parsed, dict) and isinstance(parsed.get("healthy"), bool):
        return {**parsed, "format": "json"}
    return None


async def _probe(
    world: w.World, deployment_id: str, run_id: str, namespace: str | None
) -> dict[str, Any] | None:
    entries = _cache.setdefault(world, OrderedDict())
    key = (deployment_id, namespace)
    now = time.monotonic()
    at, previous = entries.get(key, (0.0, None))
    if previous is not None and now - at < _PROBE_CACHE_TTL:
        return dict(previous)
    recent_miss = key in entries and previous is None and now - at < _PROBE_MISS_TTL
    answer = None
    with anyio.move_on_after(_PROBE_RETRY_TIMEOUT if recent_miss else _PROBE_TIMEOUT):
        try:
            correlation = world.create_run_id()
            await world.queue(
                w.get_queue_name("health_check", namespace),
                w.HealthCheckPayload(
                    health_check=True, correlation_id=correlation, probe_run_id=run_id
                ),
                deployment_id=deployment_id,
                spec_version=1,
            )
            while answer is None:
                try:
                    with anyio.move_on_after(1):
                        async with contextlib.aclosing(
                            world.streams_get(
                                f"wrun_hc_{correlation}", f"__health_check__{correlation}"
                            )
                        ) as stream:
                            body = b"".join([chunk async for chunk in stream])
                        answer = _parse_response(body)
                except NotImplementedError:
                    break
                except Exception:
                    pass
                if answer is None:
                    await anyio.sleep(0.1)
        except Exception:
            pass
    entries.pop(key, None)
    entries[key] = (
        at if answer is None and recent_miss else time.monotonic(),
        {k: v for k, v in answer.items() if k != "encryptionPublicKey"}
        if answer is not None
        else None,
    )
    if len(entries) > _PROBE_CACHE_MAX_ENTRIES:
        entries.popitem(last=False)
    global _warned
    if answer is None and not _warned:
        logger.warning(
            "Target deployment did not answer the capability probe; using fallback spec version %s",
            _spec_version(None),
        )
        _warned = True
    return answer


def _spec_version(answer: dict[str, Any] | None) -> int:
    value = (answer or {}).get("specVersion")
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value >= 1
        and value % 1 == 0
    ):
        target = int(value)
    elif answer and answer.get("format") == "json":
        target = 3
    elif answer and answer.get("healthy"):
        target = 2
    else:
        # TS uses slot identity as its probe-miss floor, capped to the caller.
        target = 6
    return min(target, w.SPEC_VERSION_CURRENT)


def _compression_mode(answer: dict[str, Any] | None) -> CompressionMode:
    version = (answer or {}).get("workflowCoreVersion")
    if not isinstance(version, str):
        return "none"
    try:
        core_version = semver.Version.parse(version.removeprefix("v"))
    except ValueError:
        return "none"
    if core_version < _COMPRESSION_MIN_VERSION:
        return "none"
    node = (answer or {}).get("nodeVersion")
    if isinstance(node, str):
        try:
            node_version = semver.Version.parse(node.removeprefix("v"))
        except ValueError:
            pass
        else:
            if node_version.prerelease is None and (
                node_version >= semver.Version(23, 8, 0)
                or semver.Version(22, 15, 0) <= node_version < semver.Version(23)
            ):
                return "auto"
    return "gzip"
