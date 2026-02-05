.PHONY: fmt lint type test qa golden
fmt:
	uv run ruff format .
lint:
	uv run ruff check .
type:
	uv run mypy .
test:
	uv run pytest -q
qa: fmt lint type test
golden:
	./scripts/golden_baseline.sh $${DAY} $${ROOT}
