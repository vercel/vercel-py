Make `HookEvent` an async context manager

This matches TS, which supports `using`.
```
# disposes the hook on block exit
async with SomeHook.wait(...) as hook:
    res = await hook
```
