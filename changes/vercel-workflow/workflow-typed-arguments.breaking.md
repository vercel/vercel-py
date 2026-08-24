Use type annotations on workflows and step to allow passing Pydantic models and dataclasses.

This is a breaking change, because type annotations will now be
enforced. Passing a `dict` when the declaration expects a `list` will
fail.

Pydantic models and dataclasses can no longer be passed to
`@serializable` or `register_serializable()`. Annotate the workflow or step
parameter or return value with their type instead.
