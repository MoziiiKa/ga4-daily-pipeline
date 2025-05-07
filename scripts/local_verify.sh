#!/usr/bin/env bash
set -euo pipefail

echo "⏺ Running ruff lint ..."
ruff check src tests                          # lint

echo "⏺ Running black style check ..."
black --check src tests                 # format check

echo "⏺ Running pytest ..."
pytest -q                               # unit tests
echo "✅ All local checks passed"
