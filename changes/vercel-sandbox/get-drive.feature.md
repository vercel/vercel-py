Add `get_drive(name_or_id=...)` to look up existing Drives by name or ID without creating them, with async and sync support. Missing Drives raise `SandboxApiError` with status code 404.
