# Security Guide

## Threat Model

| Threat | Mitigation |
|--------|-----------|
| Credential leak in repo | `.env` forbidden by CI, secret scanning |
| RPC abuse | Rate limiting, circuit breaker, no placeholders |
| SQL injection | SQLAlchemy ORM, parameterized queries only |
| Model poisoning | Input validation, no user-controlled paths |
| DoS via large ranges | Max 10M block span enforced |
| Supply chain attack | Hash-locked dependencies (`--require-hashes`) in CI and the image, digest-pinned base image, SBOM generation |

## CI Security Pipeline

Every step below is a gate: a finding fails the pipeline.

1. **Pre-flight**: secret scan (TruffleHog), forbidden file check; commit signatures are reported
2. **Dependency audit**: pip-audit on the hash-locked environment; SBOM generation
3. **Static analysis**: ruff lint and format check, mypy (strict), bandit
4. **Test**: pytest with 80% coverage minimum; integration tests and `alembic upgrade head` on PostgreSQL; detection-quality benchmark
5. **Container**: Trivy fails the build on any HIGH/CRITICAL vulnerability with a fix available (all findings are uploaded as SARIF)

## Container

- Multi-stage build: no compiler, pip or setuptools in the runtime image
- Base image pinned by digest; security fixes it predates are pinned to the fixed Debian package version in the Dockerfile
- Runs as a non-root user; `HEALTHCHECK` on the API's `/health/live`
- Health probes are unauthenticated and say only up/down and, for readiness,
  which check failed (a fixed reason word, never an error message or URL)

## Runtime Security

- Database SSL mode `require` minimum
- No plaintext credentials in logs, metrics or traces: configured RPC URLs
  (which carry provider API keys) are redacted from every log line and from
  error messages the API returns, and never recorded in metrics or spans
- `/metrics` requires the API key whenever authentication is on
- Alert webhooks can be signed (HMAC-SHA256 over timestamp and body, see
  [alerting.md](alerting.md)); alert webhook URLs, which are credentials, are
  redacted from logs like the RPC URLs
- Circuit breaker prevents retry storms
- Wrong API keys lock the client out with a doubling backoff on every route,
  the panel login and websockets (even the right key is refused while locked
  out); per-IP rate limiting. Both can be shared by all instances through
  Redis (`RATE_LIMIT_REDIS_URL`), whose URL is redacted from logs like the
  others
- Background audits are bounded (`AUDIT_MAX_CONCURRENT`, one per pool),
  persisted, and expire (`AUDIT_TASK_TTL_HOURS`)
- Input validation fails fast (no partial processing)

## Responsible Disclosure

Report security issues to: [security contact]
