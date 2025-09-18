import enum


class StatusCode(enum.Enum):
    OK = 0

    """Internal errors."""
    INTERNAL = 13


class Status:
    def __init__(self, canonical_code=StatusCode.OK):
        self._canonical_code = canonical_code

    @property
    def canonical_code(self) -> StatusCode:
        return self._canonical_code

    @property
    def is_ok(self) -> bool:
        return self._canonical_code == StatusCode.OK
