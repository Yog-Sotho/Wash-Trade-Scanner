"""
Custom exceptions for the wash trade detection system.
"""


class WashTradeError(Exception):
    """Base exception for all wash trade detection errors."""

    pass


class ConfigurationError(WashTradeError):
    """Raised when configuration is invalid or missing."""

    pass


class RPCError(WashTradeError):
    """Raised when RPC call fails."""

    pass


class RPCRateLimitError(RPCError):
    """Raised when RPC rate limit is hit."""

    pass


class CircuitBreakerOpen(WashTradeError):
    """Raised when circuit breaker is open."""

    pass


class DatabaseError(WashTradeError):
    """Raised when database operation fails."""

    pass


class ValidationError(WashTradeError):
    """Raised when input validation fails."""

    pass


class ModelNotTrainedError(WashTradeError):
    """Raised when ML model is used before training."""

    pass


class InsufficientDataError(WashTradeError):
    """Raised when insufficient data for detection."""

    pass
