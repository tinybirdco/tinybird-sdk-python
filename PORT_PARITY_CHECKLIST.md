# Python Port Parity Checklist

Generated: 2026-02-14

## Goal
Reach feature-complete parity with the latest TypeScript SDK from:
`/Users/rafa/tinybird/tinybird-full-ts`

Target Python repo:
`/Users/rafa/.superset/worktrees/tinybird/rafa-moreno/porting-tinybird-typescript-sdk-to-python`

## Snapshot
- TS runtime files (non-test): `67`
- TS test files: `43`
- Python runtime files: `79`
- Python test files: `15`
- Python baseline tests: `56 passed`

## Priority Summary
- P0: CLI subsystem parity (`src/cli/*`, `src/cli/commands/*`)
- P0: Migration subsystem parity (`src/migrate/*`)
- P1: Root export parity (`src/index.ts` vs `src/tinybird_sdk/__init__.py`)
- P1: Generator include/loader parity (`src/generator/include-paths.ts`, `src/generator/loader.ts`)
- P1: Config behavior parity (`src/cli/config.ts` vs `src/tinybird_sdk/cli/config.py`)
- P2: Test coverage parity and fixture parity

## Module Parity Matrix

| Area | TS Source | Python Source | Status | Notes |
|---|---|---|---|---|
| Root API exports | `src/index.ts` | `src/tinybird_sdk/__init__.py` | Complete | Runtime export parity validated with tests |
| API layer | `src/api/*` | `src/tinybird_sdk/api/*` | Complete | Expanded API parity tests + bugfixes |
| Client layer | `src/client/*` | `src/tinybird_sdk/client/*` | Complete | Added parity tests and error-path coverage |
| Schema DSL | `src/schema/*` | `src/tinybird_sdk/schema/*` | Complete | Root/schema helper coverage expanded |
| Generator | `src/generator/*` | `src/tinybird_sdk/generator/*` | Complete | Include resolver, loader, watch mode, import path tests |
| Codegen | `src/codegen/*` | `src/tinybird_sdk/codegen/*` | Complete | Added golden fixtures and mapper/util tests |
| CLI core | `src/cli/index.ts` + supports | `src/tinybird_sdk/cli/*` | Complete | Dispatcher + support modules + command routing tests |
| CLI commands | `src/cli/commands/*` | `src/tinybird_sdk/cli/commands/*` | Complete | 12 command modules ported with success/error tests |
| Migration | `src/migrate/*` | `src/tinybird_sdk/migrate/*` | Complete | Full parser/discovery/emitter/runner + golden tests |
| Infer | `src/infer/index.ts` | `src/tinybird_sdk/infer/index.py` | Complete | Infer helper parity tests added |

## P0 Checklist

### 1) CLI subsystem parity
TS references:
- CLI entry wiring: `src/cli/index.ts:1`
- NPM bin entry: `package.json:9`
- Command modules: `src/cli/commands/*.ts`

Python current state:
- Full CLI package present, including dispatcher and command modules:
  `src/tinybird_sdk/cli/index.py`, `src/tinybird_sdk/cli/commands/*`
- Console script entrypoint configured in packaging:
  `pyproject.toml` -> `tinybird = "tinybird_sdk.cli.index:main"`

Tasks:
- [x] Add Python console script entrypoint `tinybird` in `pyproject.toml`.
- [x] Add CLI dispatcher module equivalent to `src/cli/index.ts`.
- [x] Port command modules:
  - [x] `branch`
  - [x] `build`
  - [x] `clear`
  - [x] `deploy`
  - [x] `dev`
  - [x] `info`
  - [x] `init`
  - [x] `login`
  - [x] `migrate`
  - [x] `open-dashboard`
  - [x] `preview`
  - [x] `pull`
- [x] Port CLI support modules:
  - [x] `auth`
  - [x] `branch-store`
  - [x] `config-loader`
  - [x] `env`
  - [x] `output`
  - [x] `region-selector`
  - [x] utils: `package-manager`, `schema-validation`
- [x] Add command-level parity tests for each command path.

Acceptance criteria:
- Running `tinybird --help` shows command set equivalent to TS CLI.
- Each command path has success and error-path tests.
- CLI workflows (`init` -> `build` -> `deploy` / `dev`) pass on local fixtures.

### 2) Migration subsystem parity
TS references:
- `src/migrate/discovery.ts`
- `src/migrate/emit-ts.ts`
- `src/migrate/parse.ts`
- `src/migrate/parse-datasource.ts`
- `src/migrate/parse-pipe.ts`
- `src/migrate/parse-connection.ts`
- `src/migrate/parser-utils.ts`
- `src/migrate/types.ts`

Python current state:
- Full `src/tinybird_sdk/migrate` package present with parser/discovery/emitter/runner modules.

Tasks:
- [x] Create `src/tinybird_sdk/migrate/` package with module parity.
- [x] Port parser utilities and structured parse errors.
- [x] Port resource discovery and file classification.
- [x] Port datasource/pipe/connection parsers.
- [x] Port TS emitter equivalent for migrated definitions.
- [x] Integrate migrate flow into Python CLI `migrate` command.
- [x] Add fixture-based golden tests for parser/emitter parity.

Acceptance criteria:
- Same input datafiles produce semantically equivalent migrated output.
- Parse errors include location/context parity with TS behavior.

## P1 Checklist

### 3) Root export parity
TS reference:
- Root export surface: `src/index.ts:227`, `src/index.ts:256`

Python current state:
- Root export surface is aligned to TS runtime symbols through lazy exports in `src/tinybird_sdk/__init__.py`.

Missing root exports in Python:
- [x] `column`
- [x] `createKafkaConnection`
- [x] `defineToken`
- [x] `getBranchDashboardUrl`
- [x] `getColumnJsonPath`
- [x] `getColumnNames`
- [x] `getColumnType`
- [x] `getConnectionType`
- [x] `getCopyConfig`
- [x] `getDashboardUrl`
- [x] `getDatasource`
- [x] `getDatasourceNames`
- [x] `getEndpointConfig`
- [x] `getEngineClause`
- [x] `getLocalDashboardUrl`
- [x] `getMaterializedConfig`
- [x] `getModifiers`
- [x] `getNode`
- [x] `getNodeNames`
- [x] `getParamDefault`
- [x] `getParamDescription`
- [x] `getParamTinybirdType`
- [x] `getPipe`
- [x] `getPipeNames`
- [x] `getPrimaryKey`
- [x] `getSortingKey`
- [x] `getTinybirdType`
- [x] `isConnectionDefinition`
- [x] `isCopyPipe`
- [x] `isDatasourceDefinition`
- [x] `isKafkaConnectionDefinition`
- [x] `isMaterializedView`
- [x] `isNodeDefinition`
- [x] `isParamRequired`
- [x] `isParamValidator`
- [x] `isPipeDefinition`
- [x] `isProjectDefinition`
- [x] `isTokenDefinition`
- [x] `isTypeValidator`
- [x] `parseApiUrl`
- [x] `sql`

Acceptance criteria:
- Public symbol inventory from Python root equals TS runtime export inventory (excluding TS-only type exports).

### 4) Generator include/loader parity
TS references:
- Include resolution and glob expansion: `src/generator/include-paths.ts:161`
- Entity loader and raw datafile support: `src/generator/loader.ts:213`
- Client import path computation: `src/generator/client.ts:35`

Python current state:
- Include resolver and loader modules implemented:
  `src/tinybird_sdk/generator/include_paths.py`, `src/tinybird_sdk/generator/loader.py`
- `buildFromInclude` now supports loaded entities and raw datafiles.
- Generated client computes relative imports from the configured output path.

Tasks:
- [x] Add include resolver with glob support and deterministic ordering.
- [x] Add loader that can discover entities and raw `.datasource`/`.pipe` files.
- [x] Add watch-mode equivalent (if parity target includes CLI `dev`).
- [x] Update client generation to compute relative imports from output path.

Acceptance criteria:
- Include patterns behave like TS for both direct files and globs.
- Generated client imports are valid regardless of chosen output path.

### 5) Config behavior parity
TS references:
- Env file loading + interpolation: `src/cli/config.ts:83`, `src/cli/config.ts:148`, `src/cli/config.ts:227`
- Async JS config loading path: `src/cli/config.ts:196`, `src/cli/config.ts:320`
- Path helpers: `src/cli/config.ts:99`, `src/cli/config.ts:108`, `src/cli/config.ts:124`, `src/cli/config.ts:138`

Python current state:
- Supports `.json/.mjs/.cjs` configs through `src/tinybird_sdk/cli/config_loader.py`.
- Loads `.env.local` then `.env` before interpolation in `src/tinybird_sdk/cli/config.py`.
- Path helper utilities (`getTinybirdDir`, `getDatasourcesPath`, `getPipesPath`, `getClientPath`) implemented.

Tasks:
- [x] Define parity behavior for JS configs (`mjs/cjs`) in Python.
- [x] Implement env-file loading order (`.env.local` then `.env`).
- [x] Port project-path helper utilities needed by `init` and codegen flows.

Acceptance criteria:
- Config resolution behavior matches TS for same fixtures.

## P2 Checklist

### 6) Test parity expansion
Current state:
- TS tests by area: api 10, client 3, schema 8, generator 4, codegen 3, cli 15.
- Python tests: expanded parity suite across API/client/generator/codegen/cli/migrate/infer (56 passing tests).

Tasks:
- [x] Add domain test files mirroring TS areas (api, client, schema, generator, codegen, cli, migrate).
- [x] Add golden fixtures for codegen and migration outputs.
- [x] Add end-to-end CLI tests for init/build/deploy/pull/migrate/dev.

Acceptance criteria:
- Python test suite covers all module groups and command workflows.
- Parity fixtures pass against both TS expected behavior and Python output.

## Execution Order
1. P0 CLI skeleton + command routing.
2. P0 migrate parser/emitter package.
3. P1 root export parity and compat aliases.
4. P1 generator include/loader parity.
5. P1 config behavior parity.
6. P2 test suite expansion and fixture lock.

## Definition of Done (100% Port)
- Runtime symbol parity (root API + submodules) achieved.
- CLI command and workflow parity achieved.
- Migration workflow parity achieved.
- Generator/codegen behavior parity achieved.
- Test coverage includes all major modules and command flows.
- Documentation updated to reflect Python-first workflows with parity guarantees.
