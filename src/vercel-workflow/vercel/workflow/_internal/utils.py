def utf16_code_unit_length(value: str) -> int:
    """Count UTF-16 code units, matching JavaScript string length."""
    return sum(2 if ord(char) > 0xFFFF else 1 for char in value)
