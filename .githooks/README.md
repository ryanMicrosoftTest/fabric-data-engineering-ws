# Shared git hooks

## What this does
`pre-commit` scans the **staged** content of every commit and blocks it if it finds:

- **Never-commit files** by name: `do_not_commit*`, `local.settings.json`, `.env*`, `*.pem`, `*.key`, `*.pfx`, `id_rsa`, etc.
- **High-confidence secrets**: private keys, storage `AccountKey=` / `SharedAccessKey=`, SAS `sig=`, JWTs, AWS/GitHub/Slack/OpenAI tokens, and `client_secret=/api_key=/password=` assignments.
- **Foreign tenant / customer references**: any `*.onmicrosoft.com` tenant other than this environment's own `MngEnvMCAP372892`.

It intentionally does **not** flag your own demo-tenant IDs or the synthetic
de-identification sample data (fake PHI/PII used by the masking demos).

## Enable it (once per clone)
```bash
# bash / Git Bash
./.githooks/install.sh
```
```powershell
# PowerShell
./.githooks/install.ps1
```
Or directly: `git config core.hooksPath .githooks`

## Bypass / allowlist
- Bypass one commit: `git commit --no-verify`
- Allowlist a specific line: append `# pragma: allowlist secret` to it.

## Tuning
Edit the `SECRET_PATTERNS`, `FILENAME_BLOCK`, and `OWN_TENANT` variables at the
top of `.githooks/pre-commit`.
