# Contributing to tinybird-sdk-python

Thank you for contributing.

## Prerequisites

- Python 3.11+
- `uv` installed (`pip install uv`)

## Local Setup

```bash
uv sync --group dev
uv run pre-commit install
```

## Validation Workflow (must pass before PR)

Run the same checks used in CI:

```bash
make check
```

Or run them individually:

```bash
make lint
make typecheck
make test
make secrets
```

## Pull Request Process

1. Branch from `main`.
2. Keep changes focused and include tests for behavior changes.
3. Update docs (`README.md`, this file) when usage/workflow changes.
4. Update `CHANGELOG.md` for user-facing changes.
5. Open a PR using the provided template and complete the checklist.

`CODEOWNERS` is enabled for source, tests, and release/config paths.

## Reporting Issues

Open an issue with:

- clear title and expected behavior
- reproduction steps
- environment details (Python version, OS)
- logs or stack traces when available

## Security

Do not report vulnerabilities in public issues. See `SECURITY.md`.

## License

By contributing, you agree that your contributions are licensed under MIT.
