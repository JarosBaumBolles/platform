"""Base Integration errors"""


class MalformedConfig(Exception):
    """Exception class specific to this package."""


class ConfigValidationError(Exception):
    """Exception class specific to this package."""


class EmptyRawFile(Exception):
    """Exception class specific to this package."""
