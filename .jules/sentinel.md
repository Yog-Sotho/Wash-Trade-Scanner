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

## 2025-05-20 - Lack of RPC Protocol and Chain ID Validation
**Vulnerability:** The system accepted any RPC URL protocol and did not verify that the connected node matched the expected blockchain. This could allow insecure connections (SSRF) or cross-chain data corruption/audit errors if a wrong endpoint was provided.
**Learning:** Establishing a connection is not enough; the identity and integrity of the remote service must be verified against the local configuration.
**Prevention:** Enforce strict protocol allowlists (e.g., http/https) and perform an initial handshake to verify the remote system's identity (e.g., Chain ID) before proceeding with operations.
