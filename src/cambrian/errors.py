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
