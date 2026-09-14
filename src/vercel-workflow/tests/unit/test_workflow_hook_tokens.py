import pytest

from vercel.workflow import BaseHook
from vercel.workflow._internal import core, runtime

registry = core.Workflows(as_vercel_job=False)


def make_context() -> runtime.WorkflowOrchestratorContext:
    return runtime.WorkflowOrchestratorContext(
        [], run_id="wrun_test", seed="seed", started_at=0, registry=registry
    )


def test_empty_token_is_rejected_without_changing_replay_state() -> None:
    context = make_context()
    context_token = context._ctx.set(context)
    try:
        with pytest.raises(ValueError, match="empty string token"):
            BaseHook.wait(token="")

        assert not context.hooks
        assert not context.suspensions
        hook = BaseHook.wait()
    finally:
        context._ctx.reset(context_token)

    # Catching the error must not consume IDs or randomness for later hooks.
    replay_hook = make_context().create_hook(None, BaseHook)
    assert hook._token == replay_hook._token
    assert hook._correlation_id == replay_hook._correlation_id


@pytest.mark.parametrize("options", [{}, {"token": None}, {"token": "order:42"}, {"token": " "}])
def test_valid_tokens_are_preserved_or_generated_deterministically(options) -> None:
    hooks = []
    for _ in range(2):
        context = make_context()
        context_token = context._ctx.set(context)
        try:
            hook = BaseHook.wait(**options)
        finally:
            context._ctx.reset(context_token)
        assert context.hooks[hook._correlation_id].token == hook._token
        hooks.append(hook)

    assert hooks[0]._token == hooks[1]._token
    if options.get("token") is None:
        assert len(hooks[0]._token) == 21
    else:
        assert hooks[0]._token == options["token"]
