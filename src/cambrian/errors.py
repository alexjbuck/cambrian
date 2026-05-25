"""Typed exceptions raised by cambrian.

Each exception carries a user-facing hint or doc link. Populated as milestones land.
"""


class CambrianError(Exception):
    """Base class for all cambrian exceptions."""


class ConfigNotFoundError(CambrianError):
    """Raised when the cambrian config file does not exist at the requested path."""


class InvalidConfigError(CambrianError):
    """Raised when the cambrian config fails schema validation or TOML parsing."""


class MissingEnvVarError(CambrianError):
    """Raised when ``${VAR}`` interpolation in config references unset environment variables."""


class NotInitializedError(CambrianError):
    """Raised when a sidecar-using command runs against a catalog that has never been initialised.

    The fix is always ``cambrian init``; the error message reflects that.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(
            message or "sidecar not initialized in this catalog; run `cambrian init` first"
        )


class SidecarVersionAheadError(CambrianError):
    """Raised when the sidecar's persisted version is newer than this binary understands.

    Carries the on-disk and expected versions so callers can format a useful message.
    Read-only commands may catch this and proceed; mutating commands let it bubble.
    """

    def __init__(self, found_version: int, expected_version: int) -> None:
        self.found_version = found_version
        self.expected_version = expected_version
        super().__init__(
            f"sidecar is at version {found_version} but this cambrian only understands "
            f"up to version {expected_version}; upgrade cambrian to proceed"
        )
