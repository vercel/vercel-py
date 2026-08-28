Make `await hook` never return `None`

Raises a new `HookDisposedError` instead of returning `None` when the hook has been disposed. It is now typed to return `T` instead of `T | None`. `async for` over a hook will stop iterating on disposal, still.
