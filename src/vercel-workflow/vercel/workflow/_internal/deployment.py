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
_PROBE_RECENT_MISS_TIMEOUT = 2
_PROBE_RETRY_INTERVAL = 0.1
_PROBE_CACHE_TTL = 600
_PROBE_MISS_TTL = 60
_PROBE_CACHE_MAX_ENTRIES = 256
_COMPRESSION_MIN_VERSION = semver.Version.parse("5.0.0-beta.18")


@dataclass(frozen=True)
class _ProbeCacheEntry:
    expires_at: float
    response: dict[str, Any] | None  # None: no valid probe reply; retry with a shorter timeout.


# A World scopes the deployments it can see. Run public keys are never cached.
_cache: weakref.WeakKeyDictionary[
    w.World, OrderedDict[tuple[str, str | None], _ProbeCacheEntry]
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
    """Resolve the deployment and select the capabilities used to start its run.

    The common hot path targets the current deployment. When the resolved
    target ID matches the current one, use this SDK's current spec, automatic
    compression, and local hook-resume version directly, without a capability
    probe or probe-cache lookup. This includes default starts and explicit
    IDs or ``"latest"`` that resolve to the current deployment.

    Cross-deployment capability discovery is best effort. Without a valid
    cached or probed reply, use the slot-identity fallback capped at this
    writer's supported spec, disable compression, and omit unattested runtime
    markers. Startup may continue, but compatibility is unconfirmed: an older
    target may reject the resulting run.

    Without a probed public key, the encoder uses the regular run-key lookup;
    lookup failures still propagate.
    """
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
    """Read versioned JSON capabilities or a legacy plain-text health reply."""
    if not body:
        return None
    try:
        parsed = json.loads(body.decode("utf-8", errors="replace"))
    except ValueError:
        # Before spec 3, responders wrote plain text such as 'Workflow SDK
        # "..." endpoint is healthy'. Like TS, accept any non-empty text on
        # this dedicated response stream as liveness, with no capability claims.
        return {"healthy": True, "format": "text"}
    if isinstance(parsed, dict) and isinstance(parsed.get("healthy"), bool):
        return {**parsed, "format": "json"}
    return None


async def _probe(
    world: w.World, deployment_id: str, run_id: str, namespace: str | None
) -> dict[str, Any] | None:
    """Discover deployment capabilities on a best-effort basis.

    Return an unexpired cached response if available. Otherwise, probe the
    target, using a shorter timeout if an unexpired cache entry records that
    a previous attempt produced no valid reply.

    If the probe yields no valid reply, return ``None`` so ``resolve`` can
    continue startup with fallback capabilities. Those capabilities may be
    incompatible with the target; probing does not guarantee execution.
    Caller cancellation still propagates.
    """
    entries = _cache.setdefault(world, OrderedDict())
    key = (deployment_id, namespace)
    cache_entry = entries.get(key)
    if cache_entry is not None and time.monotonic() >= cache_entry.expires_at:
        cache_entry = None
    if cache_entry is not None and cache_entry.response is not None:
        return dict(cache_entry.response)

    timeout = _PROBE_RECENT_MISS_TIMEOUT if cache_entry is not None else _PROBE_TIMEOUT
    answer = await _probe_uncached(world, deployment_id, run_id, namespace, timeout=timeout)

    if answer is not None:
        entry = _ProbeCacheEntry(
            expires_at=time.monotonic() + _PROBE_CACHE_TTL,
            response={k: v for k, v in answer.items() if k != "encryptionPublicKey"},
        )
    elif cache_entry is not None:
        # Preserve the original expiry of the shorter-timeout window.
        entry = cache_entry
    else:
        entry = _ProbeCacheEntry(
            expires_at=time.monotonic() + _PROBE_MISS_TTL,
            response=None,
        )
    entries[key] = entry
    entries.move_to_end(key)
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


async def _probe_uncached(
    world: w.World,
    deployment_id: str,
    run_id: str,
    namespace: str | None,
    *,
    timeout: float,
) -> dict[str, Any] | None:
    """Request startup capabilities from a deployment on a best-effort basis.

    Return a valid health response, optionally including a public key for
    ``run_id``. Return ``None`` if the probe fails or no valid response arrives
    within ``timeout`` seconds. Caller cancellation propagates.
    """
    with anyio.move_on_after(timeout):
        try:
            correlation = world.create_run_id()
            await world.queue(
                w.get_queue_name("health_check", namespace),
                w.HealthCheckPayload(
                    health_check=True, correlation_id=correlation, probe_run_id=run_id
                ),
                deployment_id=deployment_id,
                spec_version=1,  # Use the oldest protocol until the target's version is known.
            )
        except Exception as error:
            logger.debug("Could not publish capability probe to %s: %s", deployment_id, error)
            return None

        while True:
            try:
                async with contextlib.aclosing(
                    world.streams_get(f"wrun_hc_{correlation}", f"__health_check__{correlation}")
                ) as stream:
                    body = b"".join([chunk async for chunk in stream])
                answer = _parse_response(body)
            except NotImplementedError:
                return None
            except Exception as error:
                logger.debug(
                    "Could not read capability probe response from %s; retrying: %s",
                    deployment_id,
                    error,
                )
            else:
                if answer is not None:
                    return answer
            await anyio.sleep(_PROBE_RETRY_INTERVAL)
    return None


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
