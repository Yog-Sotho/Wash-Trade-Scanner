# Sentinel's Journal - Critical Learnings

## 2025-05-15 - Insecure DATABASE_URL construction
**Vulnerability:** Manual f-string construction of `DATABASE_URL` failed to escape special characters in passwords (e.g., '@'), leading to misparsed connection strings and potential connection failures.
**Learning:** Using `sqlalchemy.engine.URL.create` is the secure and robust way to build database URLs as it handles necessary escaping of all components.
**Prevention:** Always use dedicated library functions for URI/URL construction instead of manual string concatenation or f-strings when dealing with credentials.

## 2025-06-10 - Credential leakage via plaintext DATABASE_URL
**Vulnerability:** The `DATABASE_URL` property explicitly rendered the connection string with `hide_password=False`, causing plain-text database credentials to be exposed if the URL was logged or printed.
**Learning:** `sqlalchemy.engine.URL` objects mask passwords by default when converted to strings, providing a safer alternative to raw connection strings for logging and error reporting.
**Prevention:** Avoid rendering sensitive connection strings into plain text; prefer passing around `URL` objects and only reveal credentials explicitly when already necessary.

## 2025-07-20 - Resource exhaustion (DoS) in data cleanup
**Vulnerability:** The `cleanup_old_data` method loaded all expiring records into memory before deleting them one-by-one (N+1 deletes). In a high-volume system, this could lead to memory exhaustion and database deadlocks.
**Learning:** Bulk operations should be performed using single `delete` or `update` statements at the database level to ensure atomicity and resource efficiency.
**Prevention:** Use `sqlalchemy.delete(Model).where(...)` for purging large datasets instead of iterating over ORM objects.

## 2025-05-19 - Path traversal in audit report exports
**Vulnerability:** The `_export_results` function in `scripts/run_audit.py` used the user-provided `export_path` directly in `open()`, allowing an attacker to write files to arbitrary locations on the file system.
**Learning:** Even internal-use scripts must sanitize file paths derived from user input (CLI arguments) to prevent directory traversal.
**Prevention:** Use `os.path.basename()` to strip directory components from user-provided filenames when exports should be restricted to a specific directory, or validate that the resolved path stays within an allowed base directory.

## 2025-08-10 - Credential leakage via plaintext RPC URLs
**Vulnerability:** RPC URLs often contain sensitive API keys (e.g., Infura, Alchemy). Storing them as plain strings in the configuration allowed them to be exposed in logs, console output, or when debugging the configuration object.
**Learning:** Pydantic's `SecretStr` type provides a standard way to mask sensitive values automatically in string representations while still allowing access to the raw value when needed.
**Prevention:** Use `SecretStr` for any configuration field that contains credentials or API keys. Ensure consumers explicitly call `.get_secret_value()` to avoid accidental leakage.

## 2025-09-12 - Block range limit bypass (DoS)
**Vulnerability:** The 10,000,000 block range limit for audits could be bypassed when `start_block` or `end_block` was `None`, as the Pydantic `field_validator` only checked the range if both were explicitly provided. This allowed potentially infinite scans, leading to resource exhaustion (DoS).
**Learning:** Input validation for resource limits must handle cases where values are missing or defaulted. `model_validator` is more reliable for multi-field constraints.
**Prevention:** Enforce security-critical resource limits both at the validation layer and at the execution layer after all defaults (e.g., current block height) have been resolved.

## 2026-05-30 - Stack trace disclosure and missing input bounds
**Vulnerability:** CLI scripts used `logger.exception` in global handlers, leaking full stack traces to users. Additionally, `TrainingParameters` lacked a limit on the number of pool addresses, posing a DoS risk.
**Learning:** Entry points must act as security boundaries. Using `logger.exception` is dangerous in production-facing scripts as it exposes internal code structure and environment details.
**Prevention:** Catch specific validation errors (`pydantic.ValidationError`, `ValueError`) early and provide clean error messages. Use `logger.error` for unexpected exceptions to log the message without the stack trace. Enforce `max_length` on all collection inputs in Pydantic models to prevent resource exhaustion.
