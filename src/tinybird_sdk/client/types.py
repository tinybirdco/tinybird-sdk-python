from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict


class ColumnMeta(TypedDict):
    name: str
    type: str


class QueryStatistics(TypedDict):
    elapsed: float
    rows_read: int
    bytes_read: int


class QueryResult(TypedDict):
    data: list[dict[str, Any]]
    meta: list[ColumnMeta]
    rows: int
    statistics: QueryStatistics


class IngestResult(TypedDict):
    successful_rows: int
    quarantined_rows: int


class TinybirdErrorResponse(TypedDict, total=False):
    error: str
    status: int
    documentation: str


class TinybirdError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int,
        response: TinybirdErrorResponse | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response = response

    def is_rate_limit_error(self) -> bool:
        return self.status_code == 429

    def is_auth_error(self) -> bool:
        return self.status_code in {401, 403}

    def is_not_found_error(self) -> bool:
        return self.status_code == 404

    def is_server_error(self) -> bool:
        return self.status_code >= 500


@dataclass(frozen=True, slots=True)
class ClientContext:
    token: str
    base_url: str
    dev_mode: bool
    is_branch_token: bool
    branch_name: str | None
    git_branch: str | None


class CsvDialectOptions(TypedDict, total=False):
    delimiter: str
    new_line: str
    escape_char: str


class AppendOptions(TypedDict, total=False):
    url: str
    file: str
    csv_dialect: CsvDialectOptions
    timeout: int


class AppendResult(TypedDict, total=False):
    successful_rows: int
    quarantined_rows: int
    import_id: str


class DeleteOptions(TypedDict, total=False):
    delete_condition: str
    dry_run: bool
    timeout: int


class DeleteResult(TypedDict, total=False):
    id: str
    job_id: str
    job_url: str
    status: str
    delete_id: str
    rows_to_be_deleted: int


class TruncateOptions(TypedDict, total=False):
    timeout: int


class TruncateResult(TypedDict, total=False):
    status: str


class QueryOptions(TypedDict, total=False):
    timeout: int


class IngestOptions(TypedDict, total=False):
    timeout: int
    wait: bool
    maxRetries: int
    max_retries: int


class ClientConfig(TypedDict, total=False):
    base_url: str
    token: str
    timeout: int
    dev_mode: bool
    config_dir: str
