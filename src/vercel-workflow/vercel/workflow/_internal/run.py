from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from typing import Any, Generic, Literal, ParamSpec, TypeVar, cast, overload

from . import (
    core,
    encryption,
    errors,
    serialization as ser,
    signature_codec,
    streams,
    ulid,
    world as w,
)

P = ParamSpec("P")
T = TypeVar("T")
Chunk = TypeVar("Chunk")
logger = logging.getLogger("vercel.workflow")
_generate_run_ulid = ulid.monotonic_factory()


class Run(Generic[T]):
    def __init__(
        self,
        run_id: str,
        *,
        output_codec: signature_codec.SignatureCodec | None = None,
    ) -> None:
        self._run_id = run_id
        self._world = w.get_world()
        # Only `start` has the workflow in hand; a `Run` built from a run id
        # picked up elsewhere reads its output as whatever the wire carried.
        self._codec = output_codec

    @property
    def run_id(self) -> str:
        return self._run_id

    async def status(self) -> Literal["pending", "running", "completed", "failed", "cancelled"]:
        run = await self._world.runs_get(self._run_id)
        return run.status

    async def attributes(self) -> dict[str, str]:
        run = await self._world.runs_get(self._run_id)
        return dict(run.attributes)

    async def terminate(self, *, reason: str | None = None) -> None:
        """Terminate the workflow run.

        Marks the run as ``cancelled``, preventing further workflow execution.
        Does not deliver ``CancelledError`` to the workflow or its running steps.
        Steps already executing may continue running.

        *reason* records optional plaintext on the cancellation event
        (at most 512 UTF-16 code units).
        """
        event_data = w.RunCancelledEventData(cancel_reason=reason) if reason is not None else None
        await self._world.events_create(self._run_id, w.RunCancelledEvent(event_data=event_data))

    async def _failure(self, run: w.WorkflowRun) -> Exception:
        what = f"the error of run {run.run_id}"
        try:
            key = None
            if ser.is_encrypted(run.error):
                key = await self._world.run_key(run.run_id, deployment_id=run.deployment_id)
            return ser.hydrate_error(run.error, what=what, key=key)
        except Exception as error:
            logger.debug("[Workflows] '%s' - could not read %s: %s", run.run_id, what, error)
            return RuntimeError(f"cannot read {what}: {error}")

    async def return_value(self) -> T:
        while True:
            run = await self._world.runs_get(self._run_id)
            if run.status == "completed":
                if not run.output:
                    raise RuntimeError(f"Completed workflow {run.run_id} has no output")
                key = None
                if ser.is_encrypted(run.output):
                    key = await self._world.run_key(run.run_id, deployment_id=run.deployment_id)
                output = ser.hydrate(run.output, what=f"the output of run {run.run_id}", key=key)
                if self._codec is None:
                    return cast("T", output)
                return cast("T", self._codec.validate_return(output))

            elif run.status == "cancelled":
                raise RuntimeError("workflow cancelled")

            elif run.status == "failed":
                raise errors.WorkflowRunFailedError(
                    run.run_id,
                    await self._failure(run),
                    error_code=run.error_code,
                )

            else:
                await asyncio.sleep(1)

    @overload
    def readable(
        self, *, namespace: str | None = None, start_index: int | None = None
    ) -> AsyncGenerator[Any, None]: ...

    @overload
    def readable(
        self,
        *,
        type: type[Chunk],
        namespace: str | None = None,
        start_index: int | None = None,
    ) -> AsyncGenerator[Chunk, None]: ...

    @overload
    def readable(
        self, *, type: Any, namespace: str | None = None, start_index: int | None = None
    ) -> AsyncGenerator[Any, None]: ...

    def readable(
        self,
        *,
        type: Any = None,
        namespace: str | None = None,
        start_index: int | None = None,
    ) -> AsyncGenerator[Any, None]:
        """Read what the run's steps stream, as they stream it.

        Yields one value per :meth:`~vercel.workflow.WorkflowStreamWriter.write`,
        in write order, and ends when a step closes the stream. A run that never
        closes its stream leaves this waiting until the run expires.

        Pass *type* to validate each chunk against it, the way a typed
        step argument is: a chunk a step wrote as a model comes back a model.
        A chunk that does not match raises
        :class:`~vercel.workflow.TypeValidationError` naming its index.

        *start_index* skips that many chunks; a negative value reads that many
        back from the end. Positive values resume exactly, which is what makes
        this usable behind a reconnecting client -- hand back the index you last
        saw and pass it in next time. A negative one cannot: it resolves against
        wherever the tail was at connect time, so the read is single-shot and
        will not survive a dropped connection.

        A method rather than a property, because each call opens its own read:
        iterating a property twice would quietly start a second one.
        """
        name = streams.workflow_run_stream_id(self._run_id, namespace)
        return streams._read_stream(self._world, self._run_id, name, type, start_index)

    def readable_bytes(
        self, *, namespace: str | None = None, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        """:meth:`readable`, for a stream of nothing but ``bytes``.

        The shape an HTTP body wants, so a route can hand this straight to a
        streaming response. A chunk that is not ``bytes`` is an error here
        rather than something for the response layer to trip over.
        """

        async def data() -> AsyncGenerator[bytes, None]:
            source = self.readable(namespace=namespace, start_index=start_index)
            async with contextlib.aclosing(source):
                async for value in source:
                    if not isinstance(value, bytes | bytearray):
                        raise ser.SerializationError(
                            f"Stream chunk is {type(value).__name__}, not bytes; use "
                            f"readable() for a stream of values"
                        )
                    yield bytes(value)

        return data()

    async def stream_info(self, *, namespace: str | None = None) -> w.StreamInfo:
        """The stream's last chunk index and whether it has been closed.

        ``tail_index`` is ``-1`` when nothing has been written yet, so a caller
        deriving a start index from it has to handle that rather than treat it
        as a position.
        """
        name = streams.workflow_run_stream_id(self._run_id, namespace)
        return await self._world.streams_get_info(self._run_id, name)

    async def list_streams(self) -> list[str]:
        """Every stream this run has written to, namespaced ones included."""
        return await self._world.streams_list(self._run_id)


async def start(wf: core.Workflow[P, T], *args: P.args, **kwargs: P.kwargs) -> Run[T]:
    # Bound before anything is written, so an arity mistake raises here instead
    # of leaving a run behind that fails when its body is invoked.
    bound_args, bound_kwargs = wf.bind_arguments(args, kwargs)
    dumped_args, dumped_kwargs = wf.codec.dump_arguments(bound_args, bound_kwargs)
    world = w.get_world()
    deployment_id = await world.get_deployment_id()
    namespace = wf._resolve_queue_namespace()
    # TODO: add and prefer world.create_run_id()
    run_id = f"wrun_{_generate_run_ulid()}"
    run_key = await world.run_key(run_id, deployment_id=deployment_id)
    encryption_public_key = (
        encryption.derive_run_public_key(run_key) if run_key is not None else None
    )
    input_data = ser.PayloadEncoder(compression=True, encryption_key=run_key).encode(
        ser.argument_array(dumped_args, dumped_kwargs)
    )
    execution_context: dict[str, Any] = {
        "workflowCoreVersion": w.WORKFLOW_CORE_COMPAT_VERSION,
        "hookResumeInputVersion": w.HOOK_RESUME_INPUT_VERSION,
        "features": {"encryption": run_key is not None},
    }
    if namespace is not None:
        execution_context["queueNamespace"] = namespace
    data = w.RunCreatedEventData(
        deployment_id=deployment_id,
        workflow_name=wf.workflow_id,
        input=input_data,
        execution_context=execution_context,
        encryption_public_key=encryption_public_key,
    )
    await world.events_create(run_id, data.into_event())
    await world.queue(
        w.get_queue_name(wf.workflow_id, namespace),
        w.WorkflowInvokePayload(
            run_id=run_id,
            run_input=w.RunInput(
                input=input_data,
                deployment_id=deployment_id,
                workflow_name=wf.workflow_id,
                spec_version=w.SPEC_VERSION_CURRENT,
                execution_context=execution_context,
                encryption_public_key=encryption_public_key,
                environment=world.get_environment(),
            ),
        ),
        deployment_id=deployment_id,
    )

    return Run(run_id, output_codec=wf.codec)
