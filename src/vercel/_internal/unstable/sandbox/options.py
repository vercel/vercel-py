"""Sandbox service options."""

from dataclasses import dataclass

from vercel._internal.unstable.options import ServiceOptions


@dataclass(frozen=True, slots=True)
class SandboxServiceOptions(ServiceOptions):
    base_url: str | None = None
