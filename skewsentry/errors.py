class SkewSentryError(Exception):
    """Base exception for SkewSentry."""


class ConfigurationError(SkewSentryError):
    """Raised for invalid configuration/specification."""


class AdapterError(SkewSentryError):
    """Raised for adapter execution problems."""

