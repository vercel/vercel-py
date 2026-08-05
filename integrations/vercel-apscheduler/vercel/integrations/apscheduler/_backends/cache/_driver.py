"""Cache driver: lifecycle state in one JSON document.

Single-writer-mostly semantics; the queue's idempotency keys carry the
single-successor guarantee. See the package docstring for the contract.
"""

from __future__ import annotations

from typing import Any

from contextlib import AbstractContextManager, nullcontext
from datetime import datetime, timedelta, timezone

from vercel.cache import get_cache

from ..._time import as_utc
from ..._types import (
    WAKE_REPAIR_GRACE_SECONDS,
    ClaimResult,
    DriverSnapshot,
    FinishResult,
    StartDecision,
    WakeToken,
)
from ._doc import DOC_TTL_SECONDS, from_iso, iso

UTC = timezone.utc

# A processing claim younger than this is treated as live (busy); past it,
# the owner is presumed crashed and the claim is retaken (redis lease parity).
_PROCESSING_GRACE_SECONDS = 15 * 60

_LIFECYCLE_STATES = {"running", "paused", "inactive"}

__all__ = ["CacheDriver"]


def _lifecycle_state(value: Any) -> Any:
    return value if value in _LIFECYCLE_STATES else None


def _processing_is_live(doc: dict[str, Any], now: datetime) -> bool:
    updated_at = from_iso(doc.get("updated_at"))
    if updated_at is None:
        return False
    return (now - updated_at).total_seconds() < _PROCESSING_GRACE_SECONDS


class CacheDriver:
    """Driver state in one JSON document; single-writer-mostly semantics."""

    def __init__(self, *, scope: str, scheduler_id: str, deployment: str) -> None:
        self.scope = scope
        self.scheduler_id = scheduler_id
        self.deployment = deployment
        self.key = f"aps:{scope}:{scheduler_id}:driver"
        self.tag = f"aps:{scope}:{scheduler_id}"
        self._store: Any = None

    def _read(self) -> dict[str, Any]:
        doc = get_cache().get(self.key)
        if not isinstance(doc, dict):
            return {}
        return dict(doc)

    def _write(self, doc: dict[str, Any], now: datetime) -> None:
        doc["updated_at"] = iso(now)
        get_cache().set(self.key, doc, {"ttl": DOC_TTL_SECONDS, "tags": [self.tag]})

    def _token(self, doc: dict[str, Any]) -> WakeToken | None:
        current = doc.get("current")
        if not isinstance(current, dict):
            return None
        logical_time = from_iso(current.get("logical_time"))
        if logical_time is None:
            return None
        return WakeToken(
            generation=int(doc.get("generation") or 0),
            sequence=int(current.get("sequence") or 0),
            logical_time=logical_time,
            status=str(current.get("status") or "pending"),
        )

    def _idle_lapsed(self, doc: dict[str, Any], now: datetime) -> bool:
        deadline = from_iso(doc.get("idle_expires_at"))
        return deadline is not None and doc.get("state") == "running" and now >= deadline

    def _demote_if_idle(self, doc: dict[str, Any], now: datetime) -> bool:
        if not self._idle_lapsed(doc, now):
            return False
        doc["state"] = "inactive"
        doc["current"] = None
        doc["start_status"] = None
        self._write(doc, now)
        return True

    def start(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
    ) -> StartDecision:
        return self._activate(
            now,
            manual=True,
            takeover_allowed=True,
            idle_timeout_seconds=idle_timeout_seconds,
        )

    def auto_activate(
        self,
        now: datetime,
        *,
        idle_timeout_seconds: int | None = None,
        takeover_allowed: bool = False,
    ) -> StartDecision:
        return self._activate(
            now,
            manual=False,
            takeover_allowed=takeover_allowed,
            idle_timeout_seconds=idle_timeout_seconds,
        )

    def _activate(
        self,
        now: datetime,
        *,
        manual: bool,
        takeover_allowed: bool,
        idle_timeout_seconds: int | None,
    ) -> StartDecision:
        now = as_utc(now, name="now")
        doc = self._read()
        self._demote_if_idle(doc, now)
        state = doc.get("state")
        owner = doc.get("owner_deployment")
        foreign = owner is not None and owner != self.deployment
        if foreign and not (manual or takeover_allowed):
            return StartDecision(
                generation=int(doc.get("generation") or 0),
                changed=False,
                start_status=str(doc.get("start_status") or ""),
                current_wake=self._token(doc),
                state=_lifecycle_state(state) or "running",
                owned=False,
            )
        if state == "paused" and not manual:
            if foreign:
                return StartDecision(
                    generation=int(doc.get("generation") or 0),
                    changed=False,
                    start_status=str(doc.get("start_status") or ""),
                    current_wake=self._token(doc),
                    state="paused",
                    owned=False,
                )
            if idle_timeout_seconds is not None:
                doc["idle_expires_at"] = iso(now + timedelta(seconds=idle_timeout_seconds))
            # A paused chain publishes no wakes, so nothing else refreshes the
            # document's TTL. Rewriting it here means the repeating activation
            # hook keeps a paused scheduler's flag alive for as long as the
            # deployment serves any traffic; only a fully abandoned namespace
            # is reaped by the TTL.
            self._write(doc, now)
            return StartDecision(
                generation=int(doc.get("generation") or 0),
                changed=False,
                start_status=str(doc.get("start_status") or ""),
                current_wake=self._token(doc),
                state="paused",
            )
        if state == "running" and not foreign:
            if idle_timeout_seconds is not None:
                doc["idle_expires_at"] = iso(now + timedelta(seconds=idle_timeout_seconds))
                self._write(doc, now)
            elif doc.pop("idle_expires_at", None) is not None or self._token(doc) is None:
                # A dormant chain (no current wake) has no wakes refreshing
                # its TTL; the activation hook's touch is what keeps it alive.
                self._write(doc, now)
            return StartDecision(
                generation=int(doc.get("generation") or 0),
                changed=False,
                start_status=str(doc.get("start_status") or "active"),
                current_wake=self._token(doc),
            )
        generation = int(doc.get("generation") or 0) + 1
        doc.update(
            state="running",
            generation=generation,
            owner_deployment=self.deployment,
            start_status="pending",
            activation_time=iso(now),
            current=None,
            last_sequence=0,
            dirty_logical_time=None,
        )
        if idle_timeout_seconds is not None:
            doc["idle_expires_at"] = iso(now + timedelta(seconds=idle_timeout_seconds))
        else:
            doc.pop("idle_expires_at", None)
        self._write(doc, now)
        return StartDecision(
            generation=generation,
            changed=True,
            start_status="pending",
            current_wake=None,
        )

    def pause(self, now: datetime) -> bool:
        now = as_utc(now, name="now")
        doc = self._read()
        changed = doc.get("state") != "paused"
        doc["state"] = "paused"
        doc.setdefault("generation", 0)
        self._write(doc, now)
        return changed

    def mark_start_published(self, generation: int, now: datetime) -> None:
        doc = self._read()
        if int(doc.get("generation") or 0) == generation and doc.get("start_status") == "pending":
            doc["start_status"] = "published"
            self._write(doc, as_utc(now, name="now"))

    def claim_start(self, generation: int, owner: str, now: datetime) -> ClaimResult:
        """Claim a start delivery, adopting newer generations from the message.

        Cache documents are reconstructable hints — evictable in deployments,
        per-process under ``vercel dev`` — so the message is the authority on
        chain progress. A generation ahead of the local document is adopted
        wholesale; ``paused`` fences only its own and older generations, so a
        resume (which mints a new generation) revives a paused document.
        """
        now = as_utc(now, name="now")
        doc = self._read()
        if self._demote_if_idle(doc, now):
            return ClaimResult(state="stale")
        local_generation = int(doc.get("generation") or 0)
        if generation < local_generation:
            return ClaimResult(state="stale")
        if generation == local_generation:
            if (
                doc.get("state") != "running"
                or doc.get("owner_deployment") != self.deployment
                or doc.get("start_status") in {None, "active"}
            ):
                return ClaimResult(state="stale")
            if doc.get("start_status") == "processing" and _processing_is_live(doc, now):
                return ClaimResult(state="busy")
            activation_time = from_iso(doc.get("activation_time")) or now
            doc["start_status"] = "processing"
            self._write(doc, now)
            return ClaimResult(state="claimed", activation_time=activation_time)
        doc.update(
            state="running",
            generation=generation,
            owner_deployment=self.deployment,
            start_status="processing",
            activation_time=iso(now),
            current=None,
            last_sequence=0,
            dirty_logical_time=None,
        )
        self._write(doc, now)
        return ClaimResult(state="claimed", activation_time=now)

    def finish_start(
        self,
        generation: int,
        owner: str,
        next_time: datetime | None,
        now: datetime,
    ) -> FinishResult:
        now = as_utc(now, name="now")
        doc = self._read()
        if self._demote_if_idle(doc, now):
            return FinishResult(state="fenced")
        if (
            doc.get("state") != "running"
            or int(doc.get("generation") or 0) != generation
            or doc.get("owner_deployment") != self.deployment
        ):
            return FinishResult(state="fenced")
        doc["start_status"] = "active"
        wake: WakeToken | None = None
        if next_time is not None:
            wake = WakeToken(generation=generation, sequence=1, logical_time=next_time)
            doc["current"] = {
                "sequence": 1,
                "logical_time": iso(next_time),
                "status": "pending",
            }
            doc["last_sequence"] = 1
        else:
            doc["current"] = None
            doc["last_sequence"] = 0
        doc["dirty_logical_time"] = None
        self._write(doc, now)
        return FinishResult(state="advanced", wake=wake)

    def mark_wake_published(self, generation: int, sequence: int, now: datetime) -> None:
        doc = self._read()
        current = doc.get("current")
        if (
            isinstance(current, dict)
            and int(doc.get("generation") or 0) == generation
            and int(current.get("sequence") or 0) == sequence
            and current.get("status") == "pending"
        ):
            current["status"] = "published"
            self._write(doc, as_utc(now, name="now"))

    def _current_record(self, doc: dict[str, Any], token: WakeToken) -> dict[str, Any] | None:
        """Return the mutable current-wake record iff owned and matching ``token``."""
        if doc.get("state") != "running" or doc.get("owner_deployment") != self.deployment:
            return None
        current = self._token(doc)
        record = doc.get("current")
        if current is None or not isinstance(record, dict):
            return None
        actual = (current.generation, current.sequence, current.logical_time)
        expected = (token.generation, token.sequence, token.logical_time)
        return record if actual == expected else None

    def claim_wake(self, token: WakeToken, owner: str, now: datetime) -> ClaimResult:
        """Claim a wake delivery, adopting chain progress from the message.

        Same authority rule as ``claim_start``: the local document loses to a
        strictly newer ``(generation, sequence)``. An exact match claims the
        recorded token; anything older is stale. ``paused`` fences its own
        generation only.
        """
        now = as_utc(now, name="now")
        doc = self._read()
        if self._demote_if_idle(doc, now):
            return ClaimResult(state="stale")
        record = self._current_record(doc, token)
        if record is not None:
            if record.get("status") == "processing" and _processing_is_live(doc, now):
                return ClaimResult(state="busy")
            record["status"] = "processing"
            self._write(doc, now)
            activation_time = from_iso(doc.get("activation_time")) or now
            return ClaimResult(state="claimed", activation_time=activation_time)
        local_generation = int(doc.get("generation") or 0)
        current = self._token(doc)
        # The watermark survives dormancy, so a redelivered consumed wake is
        # stale even when no current token exists.
        watermark = max(
            int(doc.get("last_sequence") or 0),
            current.sequence if current is not None else 0,
        )
        if (token.generation, token.sequence) <= (local_generation, watermark):
            return ClaimResult(state="stale")
        if token.generation == local_generation and doc.get("state") != "running":
            # Same-generation pause fences its remaining wakes.
            return ClaimResult(state="stale")
        activation_time = from_iso(doc.get("activation_time")) or now
        doc.update(
            state="running",
            generation=token.generation,
            owner_deployment=self.deployment,
            start_status="active",
            activation_time=iso(activation_time),
            current={
                "sequence": token.sequence,
                "logical_time": iso(token.logical_time),
                "status": "processing",
            },
            last_sequence=token.sequence,
            dirty_logical_time=doc.get("dirty_logical_time"),
        )
        self._write(doc, now)
        return ClaimResult(state="claimed", activation_time=activation_time)

    def finish_wake(
        self,
        token: WakeToken,
        owner: str,
        next_time: datetime | None,
        now: datetime,
    ) -> FinishResult:
        now = as_utc(now, name="now")
        doc = self._read()
        if self._demote_if_idle(doc, now):
            return FinishResult(state="fenced")
        if self._current_record(doc, token) is None:
            return FinishResult(state="fenced")
        dirty = from_iso(doc.get("dirty_logical_time"))
        if dirty is not None and (next_time is None or dirty < next_time):
            next_time = dirty
        doc["dirty_logical_time"] = None
        wake: WakeToken | None = None
        if next_time is not None:
            wake = WakeToken(
                generation=token.generation,
                sequence=token.sequence + 1,
                logical_time=next_time,
            )
            doc["current"] = {
                "sequence": token.sequence + 1,
                "logical_time": iso(next_time),
                "status": "pending",
            }
            doc["last_sequence"] = token.sequence + 1
        else:
            # Dormant, but the consumed position must stay recorded: a rearm
            # that restarted at 1 would republish under already-used
            # idempotency keys and the queue would silently drop the wake.
            doc["current"] = None
            doc["last_sequence"] = max(int(doc.get("last_sequence") or 0), token.sequence)
        self._write(doc, now)
        return FinishResult(state="advanced", wake=wake)

    def snapshot(self) -> DriverSnapshot:
        doc = self._read()
        state = doc.get("state")
        return DriverSnapshot(
            state=_lifecycle_state(state) or "inactive",
            generation=int(doc.get("generation") or 0),
            start_status=doc.get("start_status"),
            current_wake=self._token(doc),
        )

    def repair_overdue_wake(self, now: datetime) -> WakeToken | None:
        now = as_utc(now, name="now")
        doc = self._read()
        if doc.get("state") != "running" or doc.get("owner_deployment") != self.deployment:
            return None
        current = self._token(doc)
        if current is None or current.status == "pending":
            # A pending wake needs publishing, not repairing; returning None
            # keeps the loud presumed-lost warning honest.
            return None
        overdue_after = current.logical_time + timedelta(seconds=WAKE_REPAIR_GRACE_SECONDS)
        record = doc.get("current")
        if now < overdue_after or not isinstance(record, dict):
            return None
        record["status"] = "pending"
        self._write(doc, now)
        return self._token(doc)

    def owner_deployment(self) -> str | None:
        value = self._read().get("owner_deployment")
        return value if isinstance(value, str) and value else None

    def attach_store(self, store: Any) -> None:
        self._store = store

    def reconciled_deployment(self) -> str | None:
        if self._store is None:
            return None
        value = self._store._load().get("reconciled_deployment")
        return value if isinstance(value, str) and value else None

    def mark_reconciled(self, deployment: str, now: datetime) -> bool:
        del now
        # Same fence as Redis mode: a demoted straggler must not stamp the
        # marker (best-effort here, exact there). The marker is written into
        # the jobs document so eviction clears them together and the next
        # wake re-runs reconciliation instead of trusting a reaped store.
        if self._store is None or self._read().get("owner_deployment") != deployment:
            return False
        doc = self._store._load()
        doc["reconciled_deployment"] = deployment
        self._store._store(doc)
        return True

    def renew(self, owner: str, now: datetime) -> bool:
        return True

    def release(self, owner: str) -> None:
        return None

    def renewing(self, owner: str) -> AbstractContextManager[None]:
        return nullcontext()

    def rearm_wake(self, candidate: datetime, now: datetime) -> None:
        """Best-effort mirror of the Redis write fragment's rearm branch."""
        now = as_utc(now, name="now")
        candidate = as_utc(candidate, name="candidate")
        doc = self._read()
        if (
            doc.get("state") != "running"
            or doc.get("owner_deployment") != self.deployment
            or doc.get("start_status") != "active"
        ):
            return
        current = self._token(doc)
        record = doc.get("current")
        if (
            current is not None
            and isinstance(record, dict)
            and record.get("status") == "processing"
        ):
            # An in-flight wake owns the token; replacing it would race its
            # finish into a divergent payload under the same idempotency key.
            # Fold the candidate into the successor instead (redis dirty
            # parity): finish_wake takes min(dirty, computed next).
            dirty = from_iso(doc.get("dirty_logical_time"))
            if dirty is None or candidate < dirty:
                doc["dirty_logical_time"] = iso(candidate)
                self._write(doc, now)
            return
        if current is not None and current.logical_time <= candidate:
            return
        sequence = (
            max(
                int(doc.get("last_sequence") or 0),
                current.sequence if current is not None else 0,
            )
            + 1
        )
        doc["current"] = {
            "sequence": sequence,
            "logical_time": iso(candidate),
            "status": "pending",
        }
        doc["last_sequence"] = sequence
        self._write(doc, now)
