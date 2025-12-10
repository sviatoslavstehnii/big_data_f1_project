"""SQL validation and input sanitization utilities."""

import re
from typing import Optional
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    error_message: Optional[str] = None


class SQLValidator:
    """Validator for SQL queries to ensure safety and security."""

    BLOCKED_KEYWORDS = [
        "DROP",
        "DELETE",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "INSERT",
        "UPDATE",
        "MERGE",
        "GRANT",
        "REVOKE",
        "EXECUTE",
        "EXEC",
    ]

    BLOCKED_PATTERN = re.compile(
        r"\b(" + "|".join(BLOCKED_KEYWORDS) + r")\b",
        re.IGNORECASE,
    )

    INJECTION_PATTERNS = [
        r";\s*--",  # Statement terminator followed by comment
        r"'\s*OR\s+'1'\s*=\s*'1",  # Classic OR injection
        r"'\s*OR\s+1\s*=\s*1",  # Numeric OR injection
        r"UNION\s+SELECT",  # UNION injection
        r"INTO\s+OUTFILE",  # File write attempt
        r"LOAD_FILE",  # File read attempt
    ]

    def __init__(self, allowed_catalogs: Optional[list[str]] = None):
        """Initialize the validator.

        Args:
            allowed_catalogs: Optional list of allowed catalog names.
        """
        self.allowed_catalogs = allowed_catalogs or []
        self._injection_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.INJECTION_PATTERNS
        ]

    def validate_query(self, query: str) -> ValidationResult:
        if not query or not query.strip():
            return ValidationResult(
                is_valid=False,
                error_message="Query cannot be empty.",
            )

        blocked_match = self.BLOCKED_PATTERN.search(query)
        if blocked_match:
            return ValidationResult(
                is_valid=False,
                error_message=(
                    f"Query contains blocked keyword: {blocked_match.group(1).upper()}. "
                    "Only SELECT queries are allowed."
                ),
            )

        for pattern in self._injection_patterns:
            if pattern.search(query):
                return ValidationResult(
                    is_valid=False,
                    error_message="Query contains potentially dangerous pattern.",
                )

        query_stripped = query.strip().upper()
        allowed_starts = ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC")
        if not any(query_stripped.startswith(start) for start in allowed_starts):
            return ValidationResult(
                is_valid=False,
                error_message=(
                    "Query must start with SELECT, WITH, SHOW, or DESCRIBE. "
                    "Only read operations are allowed."
                ),
            )

        return ValidationResult(is_valid=True)

    def sanitize_identifier(self, identifier: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_.]", "", identifier)
        return sanitized

    def validate_table_name(self, table_name: str) -> ValidationResult:
        if not table_name or not table_name.strip():
            return ValidationResult(
                is_valid=False,
                error_message="Table name cannot be empty.",
            )

        if not re.match(r"^[a-zA-Z0-9_.]+$", table_name):
            return ValidationResult(
                is_valid=False,
                error_message=(
                    "Table name contains invalid characters. "
                    "Only alphanumeric, underscore, and dot are allowed."
                ),
            )

        if self.allowed_catalogs:
            parts = table_name.split(".")
            if len(parts) >= 2:
                catalog = parts[0]
                if catalog not in self.allowed_catalogs:
                    return ValidationResult(
                        is_valid=False,
                        error_message=f"Catalog '{catalog}' is not in the allowed list.",
                    )

        return ValidationResult(is_valid=True)


_validator: Optional[SQLValidator] = None


def get_sql_validator() -> SQLValidator:
    global _validator
    if _validator is None:
        _validator = SQLValidator()
    return _validator

