# SPY_2 Repo Rules

Run discipline
- Always run project commands with `uv run ...` unless your shell is explicitly
  activated into `.venv`.
- Reason: this guarantees the pinned interpreter and locked dependencies.

Testing
- Use `pytest -q` for touched Python modules when tests exist.
