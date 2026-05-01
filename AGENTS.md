# AGENTS.md

- Python package: `private-repo-hunter`; requires Python `>=3.14`.
- Only runnable entrypoint is `python3 rh/main.py`.
- CLI is `--dataset joss` (required) and `--output PATH` (defaults to `rh.sqlite3`).
- `rh/datasets/joss.py` is the only real data pipeline; `rh/db.py` creates the `joss` table and writes with `append`.
- Keep changes minimal in `rh/main.py`; it is the only active command path.
- Pre-commit hooks are configured for `check-*` file hygiene, `ruff-format`, `ruff-check`, `isort`, and `bandit`.
- Pre-commit’s default Python is `python3.13`, which differs from the project runtime requirement.
