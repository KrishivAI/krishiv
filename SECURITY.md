# Security Policy

## Supported versions

Krishiv is pre-1.0. Security fixes are applied to the latest release line and to
`main`. Older pre-1.0 releases may not receive backports unless maintainers
announce otherwise in the release notes.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use the repository's **Security → Report a vulnerability** private reporting
flow. Include:

- affected commit/tag and deployment mode;
- reproduction steps or proof of concept;
- expected impact and required privileges;
- relevant configuration, connector, or durability profile; and
- whether the issue is already public.

If private vulnerability reporting is unavailable on a mirror, contact the
maintainers through the private address listed by that mirror's project owner.
Do not include exploit details in public discussions.

## Response targets

These are targets, not contractual SLAs:

- acknowledgement within 3 business days;
- initial severity/impact assessment within 7 business days;
- coordinated disclosure date agreed with the reporter; and
- credit in the advisory unless anonymity is requested.

## Security boundaries

Reports are especially useful for authentication/TLS, fencing/leadership,
shuffle or checkpoint path traversal, connector credential exposure, malicious
plan/task fragments, unsafe UDF execution, state/checkpoint corruption, and
cross-tenant data disclosure.

## Hardening guidance

- Use `distributed-durable` only with consensus metadata, object-store
  checkpoints, tiered shuffle, fencing, TLS, and non-empty coordinator/executor
  bearer tokens.
- Store tokens and connector credentials in mounted secret files and rotate
  them; never commit credentials.
- Keep control-plane/UI endpoints private unless protected by an authenticated
  gateway.
- Run executors with least-privilege filesystem/object-store credentials.
- Treat experimental connectors and UDFs as untrusted until reviewed for the
  target deployment.
