"""Single source of truth for project configuration."""

import logging
import re

PROJECT_NAME = "DataConnect"

# Logging
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# Scanner defaults
DEFAULT_SAMPLE_PERCENT = 5.0
MAX_SAMPLE_ROWS = 10_000

# Router defaults
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
MAX_RELEVANT_TABLES = 8
RELATIONSHIP_DEPTH = 2

# Verifier defaults
MAX_RETRY_ATTEMPTS = 3
CONFIDENCE_HIGH = 90
CONFIDENCE_MEDIUM = 70
CONFIDENCE_LOW = 50

# API defaults
DEFAULT_API_PORT = 8000
RATE_LIMIT_PER_MINUTE = 60

# Storage
STORAGE_DB_NAME = "dataconnect_index.db"

# Connection string sanitization pattern
_CONN_PASSWORD_RE = re.compile(r"(://[^:]+:)([^@]+)(@)")


def sanitize_connection_string(conn_str: str) -> str:
    """Mask password in connection strings for safe logging."""
    return _CONN_PASSWORD_RE.sub(r"\1***\3", conn_str)
