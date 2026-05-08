# Security Guide

## Threat Model

| Threat | Mitigation |
|--------|-----------|
| Credential leak in repo | `.env` forbidden by CI, secret scanning |
| RPC abuse | Rate limiting, circuit breaker, no placeholders |
| SQL injection | SQLAlchemy ORM, parameterized queries only |
| Model poisoning | Input validation, no user-controlled paths |
| DoS via large ranges | Max 10M block span enforced |
| Supply chain attack | Pinned dependencies, SBOM generation |

## CI Security Pipeline

1. **Pre-flight**: GPG commit signatures, secret scan (TruffleHog), forbidden file check
2. **Dependency audit**: pip-audit, safety scan, SBOM generation
3. **Static analysis**: ruff lint, mypy type check, bandit security scan
4. **Test**: pytest with 80% coverage minimum
5. **Container**: Trivy vulnerability scan before push

## Runtime Security

- Database SSL mode `require` minimum
- No plaintext credentials in logs
- Circuit breaker prevents retry storms
- Input validation fails fast (no partial processing)

## Responsible Disclosure

Report security issues to: [security contact]
