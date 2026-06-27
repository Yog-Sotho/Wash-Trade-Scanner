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

## 2025-10-25 - Incomplete security parity in EntityClusterer
**Vulnerability:** The `EntityClusterer` lacked the same security protections as `ChainIngestor`, specifically missing RPC protocol validation, Chain ID verification, and block range limit enforcement. This allowed insecure protocols and potential resource exhaustion (DoS) through unbounded block scans.
**Learning:** Security controls must be applied consistently across all components that interact with external resources or perform heavy operations. Modularizing shared validation logic is better than duplicating checks.
**Prevention:** When introducing new data-fetching components, audit them against the security checklist of existing components (e.g., RPC validation, DoS limits, credential masking).

## 2026-06-27 - Denial of Service (DoS) via uninitialized variables in heuristics
**Vulnerability:** The `detect_high_frequency_bot` heuristic contained uninitialized variables (`inter_trade_times`, `volumes`) in its loop, causing a `NameError` and immediate process crash when processing valid trade data. This effectively created a DoS condition for the audit pipeline.
**Learning:** Incomplete or broken performance optimizations can introduce critical reliability bugs that function as DoS vulnerabilities. Mixing manual loops with vectorized variable names without proper initialization is a high-risk pattern.
**Prevention:** Always verify performance optimizations with reproduction scripts and edge-case tests (empty lists, single items). Prefer full NumPy vectorization over hybrid loop-vectorized approaches to reduce state-management errors.

## 2026-06-27 - Information disclosure via unmasked Pydantic validation errors
**Vulnerability:** CLI entry points (`run_audit.py`, `train_model.py`) failed to catch `pydantic.ValidationError` explicitly, allowing raw Pydantic tracebacks to reach the console. These tracebacks leaked internal model structures and file system paths.
**Learning:** Standard `try...except Exception` blocks that use `logger.exception` are insecure for user-facing entry points as they expose full stack traces by default. Pydantic errors require specific handling to present clean messages to the user.
**Prevention:** Explicitly catch `pydantic.ValidationError` and `ValueError` at the highest level of CLI scripts. Log a sanitized error message at the `ERROR` level and relegate the full traceback to `DEBUG` level using `logger.debug(..., exc_info=True)`.
