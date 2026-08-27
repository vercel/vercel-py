Prevent workflows from having side effects while suspending.

`hook.dispose()` will now work properly in a `finally` block.  (That
is, the hook will be disposed only when the workflow is actually
terminating, and not every time it gets replayed.)
