from pydantic import AfterValidator


def utf16_code_unit_length(value: str) -> int:
    """Count UTF-16 code units, matching JavaScript string length."""
    return sum(2 if ord(char) > 0xFFFF else 1 for char in value)


def Utf16MaxLength(code_units: int) -> AfterValidator:
    """Limit a string to this many UTF-16 code units, matching JS string length.

    Use as ``Annotated[str, Utf16MaxLength(512)]``. Unlike Pydantic's
    ``max_length``, a non-BMP character (such as an emoji) counts as two units.
    """

    if code_units < 0:
        raise ValueError("code_units must be non-negative")

    def validate(value: str) -> str:
        if utf16_code_unit_length(value) > code_units:
            raise ValueError(f"String must be at most {code_units} UTF-16 code units")
        return value

    return AfterValidator(validate)
