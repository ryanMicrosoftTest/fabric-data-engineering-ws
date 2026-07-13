# Enable the repo's shared git hooks. Run once per clone (PowerShell).
git config core.hooksPath .githooks
Write-Host "core.hooksPath set to .githooks. Pre-commit secret/customer scan is active." -ForegroundColor Green
