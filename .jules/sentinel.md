# Sentinel's Journal - Critical Learnings

## 2025-05-15 - Insecure DATABASE_URL construction
**Vulnerability:** Manual f-string construction of `DATABASE_URL` failed to escape special characters in passwords (e.g., '@'), leading to misparsed connection strings and potential connection failures.
**Learning:** Using `sqlalchemy.engine.URL.create` is the secure and robust way to build database URLs as it handles necessary escaping of all components.
**Prevention:** Always use dedicated library functions for URI/URL construction instead of manual string concatenation or f-strings when dealing with credentials.

## 2025-06-10 - Credential leakage via plaintext DATABASE_URL
**Vulnerability:** The `DATABASE_URL` property explicitly rendered the connection string with `hide_password=False`, causing plain-text database credentials to be exposed if the URL was logged or printed.
**Learning:** `sqlalchemy.engine.URL` objects mask passwords by default when converted to strings, providing a safer alternative to raw connection strings for logging and error reporting.
**Prevention:** Avoid rendering sensitive connection strings into plain text; prefer passing around `URL` objects and only reveal credentials explicitly when absolutely necessary.

## 2025-06-25 - Insecure RPC placeholder usage
**Vulnerability:** The application used hardcoded placeholder RPC URLs (e.g., containing "YOUR_KEY") from configuration files even when environment overrides were provided, leading to connection failures and potential exposure to insecure defaults.
**Learning:** Centralized validation of configuration fields (e.g., using Pydantic model validators) is more robust than per-field checks and ensures that security policies are applied consistently across all components.
**Prevention:** Use model-level validation for all security-sensitive configuration strings and ensure that runtime components correctly prioritize user-provided environment variables over static configuration defaults.
