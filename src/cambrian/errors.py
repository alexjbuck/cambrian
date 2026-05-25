"""Typed exceptions raised by cambrian.

Each exception carries a user-facing hint or doc link. Populated as milestones land.
"""


class CambrianError(Exception):
    """Base class for all cambrian exceptions."""
