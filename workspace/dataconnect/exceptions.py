"""Typed exceptions for DataConnect. No generic Exception raises."""


class DataConnectError(Exception):
    """Base exception for all DataConnect errors."""


# Database errors
class DatabaseConnectionError(DataConnectError):
    """Failed to connect to user database."""


class ReadOnlyViolationError(DataConnectError):
    """Attempted a non-SELECT query."""


# Scanner errors
class ScanError(DataConnectError):
    """Error during schema scanning."""


class ProfilingError(ScanError):
    """Error during data profiling."""


# Router errors
class RoutingError(DataConnectError):
    """Error during table routing."""


class EmbeddingError(RoutingError):
    """Error computing embeddings."""


# Verifier errors
class VerificationError(DataConnectError):
    """Error during SQL verification."""


class RetryExhaustedError(VerificationError):
    """Max retry attempts reached."""


# Storage errors
class StorageError(DataConnectError):
    """Error reading/writing storage index."""


# API errors
class AuthenticationError(DataConnectError):
    """Invalid or missing API key."""


class RateLimitError(DataConnectError):
    """Rate limit exceeded."""


# LLM errors
class LLMError(DataConnectError):
    """Error calling LLM provider."""


# Generation errors
class GenerationError(DataConnectError):
    """Error generating SQL from natural language."""


# Benchmark errors
class BenchmarkError(DataConnectError):
    """Error during benchmark execution."""
