SRC_DIRS=src tests

.PHONY: help
help: ## Show available commands
	@awk -F ':|##' '/^[^\t].+?:.*?##/ { printf "\033[36m%-22s\033[0m %s\n", $$1, $$NF }' $(MAKEFILE_LIST)

.PHONY: install
install: ## Install dev dependencies with uv
	uv sync --group dev

.PHONY: lint
lint: ## Run ruff lint and format checks
	uv run ruff check .
	uv run ruff format --check .

.PHONY: lint-fix
lint-fix: ## Auto-fix lint and format
	uv run ruff check . --fix
	uv run ruff format .

.PHONY: typecheck
typecheck: ## Run mypy type checks
	uv run mypy

.PHONY: test
test: ## Run test suite
	uv run pytest

.PHONY: secrets
secrets: ## Run gitleaks secret scan
	uv run pre-commit run gitleaks --all-files

.PHONY: check
check: ## Run full local CI checks
	@$(MAKE) lint
	@$(MAKE) typecheck
	@$(MAKE) test
	@$(MAKE) secrets
