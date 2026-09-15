"""Private sentinels shared by the public and internal call layers."""


class _UnsetType:
    def __repr__(self) -> str:
        return "UNSET"


UNSET = _UnsetType()


__all__ = ["UNSET"]
