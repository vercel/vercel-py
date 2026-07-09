"""Workflow-sandbox configuration and teardown cleanup handlers.

Cleanup handlers run on the host after every sandbox teardown to purge
host-shared caches that would otherwise pin the run's module graph.
None run by default; compose a policy and pass it to ``Workflows``::

    from vercel.workflow import Workflows, sandbox

    workflows = Workflows(
        sandbox_policy=sandbox.SandboxPolicy(
            cleanups=(*sandbox.ALL_CLEANUPS, my_cleanup),
        )
    )
"""

from vercel._internal.workflow.py_sandbox import (
    ALL_CLEANUPS,
    CleanupHandler,
    SandboxCleanupContext,
    SandboxPolicy,
    clear_pydantic_generics_cache,
    clear_typing_caches,
)

__all__ = [
    "ALL_CLEANUPS",
    "CleanupHandler",
    "SandboxPolicy",
    "SandboxCleanupContext",
    "clear_pydantic_generics_cache",
    "clear_typing_caches",
]
