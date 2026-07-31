Every value type on the `vercel.connect` surface is a frozen Pydantic model:
subjects, authorization details, options, and returned responses. Values are
constructed by keyword and validated on construction, a misspelled field is an
error rather than a silently dropped value, and `model_dump()` is available for
logging.

Rejections raise `ConnectValidationError`, including a failed assignment to a
frozen value, so callers never handle Pydantic's own error type.

Parameters and fields that take a container of strings — `scopes`, `audience`,
`resources`, `permissions`, `repositories` — share the `StringContainer` alias.
Any container is accepted and a tuple is stored, but a bare string is rejected
rather than expanded into one entry per character, both by a type checker and at
runtime: `scopes="repo:read"` is nine one-character scopes, not one scope.
