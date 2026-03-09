from __future__ import annotations

from .parser_utils import MigrationParseError, is_blank, parse_directive_line, parse_quoted_value, split_lines
from .types import ConnectionModel, GCSConnectionModel, KafkaConnectionModel, ResourceFile, S3ConnectionModel


def parse_connection_file(resource: ResourceFile) -> ConnectionModel:
    lines = split_lines(resource.content)
    connection_type: str | None = None

    bootstrap_servers: str | None = None
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    key: str | None = None
    secret: str | None = None
    schema_registry_url: str | None = None
    ssl_ca_pem: str | None = None

    region: str | None = None
    arn: str | None = None
    access_key: str | None = None
    access_secret: str | None = None
    service_account_credentials_json: str | None = None

    for raw_line in lines:
        line = raw_line.strip()
        if is_blank(line) or line.startswith("#"):
            continue

        directive = parse_directive_line(line)
        name = directive["key"]
        value = directive["value"]

        if name == "TYPE":
            connection_type = parse_quoted_value(value)
        elif name == "KAFKA_BOOTSTRAP_SERVERS":
            bootstrap_servers = parse_quoted_value(value)
        elif name == "KAFKA_SECURITY_PROTOCOL":
            parsed_value = parse_quoted_value(value)
            if parsed_value not in {"SASL_SSL", "PLAINTEXT", "SASL_PLAINTEXT"}:
                raise MigrationParseError(
                    resource.file_path,
                    "connection",
                    resource.name,
                    f'Unsupported KAFKA_SECURITY_PROTOCOL: "{value}"',
                )
            security_protocol = parsed_value
        elif name == "KAFKA_SASL_MECHANISM":
            parsed_value = parse_quoted_value(value)
            if parsed_value not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"}:
                raise MigrationParseError(
                    resource.file_path,
                    "connection",
                    resource.name,
                    f'Unsupported KAFKA_SASL_MECHANISM: "{value}"',
                )
            sasl_mechanism = parsed_value
        elif name == "KAFKA_KEY":
            key = parse_quoted_value(value)
        elif name == "KAFKA_SECRET":
            secret = parse_quoted_value(value)
        elif name == "KAFKA_SCHEMA_REGISTRY_URL":
            schema_registry_url = parse_quoted_value(value)
        elif name == "KAFKA_SSL_CA_PEM":
            ssl_ca_pem = parse_quoted_value(value)
        elif name == "S3_REGION":
            region = parse_quoted_value(value)
        elif name == "S3_ARN":
            arn = parse_quoted_value(value)
        elif name == "S3_ACCESS_KEY":
            access_key = parse_quoted_value(value)
        elif name == "S3_SECRET":
            access_secret = parse_quoted_value(value)
        elif name == "GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON":
            service_account_credentials_json = parse_quoted_value(value)
        else:
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                f'Unsupported connection directive in strict mode: "{line}"',
            )

    if not connection_type:
        raise MigrationParseError(resource.file_path, "connection", resource.name, "TYPE directive is required.")

    if connection_type == "kafka":
        if region or arn or access_key or access_secret or service_account_credentials_json:
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "S3/GCS directives are not valid for kafka connections.",
            )

        if not bootstrap_servers:
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "KAFKA_BOOTSTRAP_SERVERS is required for kafka connections.",
            )

        return KafkaConnectionModel(
            kind="connection",
            name=resource.name,
            file_path=resource.file_path,
            connection_type="kafka",
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,  # type: ignore[arg-type]
            sasl_mechanism=sasl_mechanism,  # type: ignore[arg-type]
            key=key,
            secret=secret,
            schema_registry_url=schema_registry_url,
            ssl_ca_pem=ssl_ca_pem,
        )

    if connection_type == "s3":
        if (
            bootstrap_servers
            or security_protocol
            or sasl_mechanism
            or key
            or secret
            or schema_registry_url
            or ssl_ca_pem
            or service_account_credentials_json
        ):
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "Kafka/GCS directives are not valid for s3 connections.",
            )

        if not region:
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "S3_REGION is required for s3 connections.",
            )

        if not arn and not (access_key and access_secret):
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "S3 connections require S3_ARN or both S3_ACCESS_KEY and S3_SECRET.",
            )

        if (access_key and not access_secret) or (not access_key and access_secret):
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "S3_ACCESS_KEY and S3_SECRET must be provided together.",
            )

        return S3ConnectionModel(
            kind="connection",
            name=resource.name,
            file_path=resource.file_path,
            connection_type="s3",
            region=region,
            arn=arn,
            access_key=access_key,
            secret=access_secret,
        )

    if connection_type == "gcs":
        if (
            bootstrap_servers
            or security_protocol
            or sasl_mechanism
            or key
            or secret
            or schema_registry_url
            or ssl_ca_pem
            or region
            or arn
            or access_key
            or access_secret
        ):
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "Kafka/S3 directives are not valid for gcs connections.",
            )

        if not service_account_credentials_json:
            raise MigrationParseError(
                resource.file_path,
                "connection",
                resource.name,
                "GCS_SERVICE_ACCOUNT_CREDENTIALS_JSON is required for gcs connections.",
            )

        return GCSConnectionModel(
            kind="connection",
            name=resource.name,
            file_path=resource.file_path,
            connection_type="gcs",
            service_account_credentials_json=service_account_credentials_json,
        )

    raise MigrationParseError(
        resource.file_path,
        "connection",
        resource.name,
        f'Unsupported connection type in strict mode: "{connection_type}"',
    )
