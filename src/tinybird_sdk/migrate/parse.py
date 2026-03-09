from __future__ import annotations

from .parse_connection import parse_connection_file
from .parse_datasource import parse_datasource_file
from .parse_pipe import parse_pipe_file
from .types import ParsedResource, ResourceFile


def parse_resource_file(resource: ResourceFile) -> ParsedResource:
    if resource.kind == "datasource":
        return parse_datasource_file(resource)
    if resource.kind == "pipe":
        return parse_pipe_file(resource)
    if resource.kind == "connection":
        return parse_connection_file(resource)
    raise ValueError(f"Unsupported resource kind: {resource.kind}")
