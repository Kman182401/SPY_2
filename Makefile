.PHONY: fmt lint type test qa
fmt:
	uv run ruff format .
lint:
	uv run ruff check .
type:
	uv run mypy .
test:
	uv run pytest -q
qa: fmt lint type test
