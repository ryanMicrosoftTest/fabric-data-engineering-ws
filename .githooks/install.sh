#!/usr/bin/env bash
# Enable the repo's shared git hooks. Run once per clone.
set -e
git config core.hooksPath .githooks
chmod +x .githooks/* 2>/dev/null || true
echo "core.hooksPath set to .githooks. Pre-commit secret/customer scan is active."
