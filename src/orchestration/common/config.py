from __future__ import annotations

import os
import re
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

from orchestration.common.env_paths import resolve_project_root


class ConnectorType(str, Enum):
    """Enum des connecteurs SQL pris en charge de manière standard."""

    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    SQLITE = "sqlite"


@dataclass(frozen=True)
class PathConfig:
    """Centralise les chemins standards utilisés dans le template."""

    project_root: Path
    dags_dir: Path
    config_dir: Path
    plugins_dir: Path
    docs_dir: Path
    monitoring_dir: Path

    @classmethod
    def from_environment(cls, base_dir: Optional[str] = None) -> "PathConfig":
        """Construit la configuration de chemins à partir de l'environnement runtime."""
        project_root = Path(base_dir) if base_dir else resolve_project_root()
        return cls(
            project_root=project_root,
            dags_dir=project_root / "dags",
            config_dir=project_root / "config",
            plugins_dir=project_root / "plugins",
            docs_dir=project_root / "docs",
            monitoring_dir=project_root / "monitoring",
        )


@dataclass(frozen=True)
class RelationalSourceConfig:
    """Représente une source de données relationnelle nommée."""

    name: str
    connector: ConnectorType
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    params: dict[str, str] = field(default_factory=dict)

    def sqlalchemy_uri(self) -> str:
        """Construit l'URI SQLAlchemy selon le connecteur de la source."""
        query_params = dict(self.params)

        if self.connector == ConnectorType.SQLITE:
            sqlite_db = self.database or ":memory:"
            if sqlite_db == ":memory:":
                return "sqlite:///:memory:"
            if sqlite_db.startswith("/"):
                return f"sqlite:///{sqlite_db}"
            return f"sqlite:///{sqlite_db}"

        if not all([self.host, self.database, self.user]):
            raise ValueError(
                f"Configuration incomplète pour la source '{self.name}': "
                "host, database et user sont requis"
            )

        encoded_user = urllib.parse.quote_plus(self.user or "")
        encoded_password = urllib.parse.quote_plus(self.password or "")
        auth = f"{encoded_user}:{encoded_password}@"
        host_port = f"{self.host}:{self.port}" if self.port else self.host

        if self.connector == ConnectorType.POSTGRES:
            driver = "postgresql+psycopg2"
        elif self.connector == ConnectorType.MYSQL:
            driver = "mysql+pymysql"
        elif self.connector == ConnectorType.MSSQL:
            driver = "mssql+pyodbc"
            query_params.setdefault("driver", "ODBC Driver 18 for SQL Server")
            query_params.setdefault("TrustServerCertificate", "yes")
        elif self.connector == ConnectorType.ORACLE:
            driver = "oracle+oracledb"
        else:
            raise ValueError(f"Connecteur non supporté: {self.connector}")

        query_string = ""
        if query_params:
            query_string = "?" + urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote_plus)

        return f"{driver}://{auth}{host_port}/{self.database}{query_string}"

    @classmethod
    def from_legacy_prefix(
        cls,
        prefix: str,
        connector: ConnectorType = ConnectorType.POSTGRES,
        default_port: Optional[int] = None,
    ) -> "RelationalSourceConfig":
        """Construit une source depuis l'ancien format `<PREFIX>_*` (ex: `DWH_*`)."""
        source_name = prefix.lower()
        host = os.getenv(f"{prefix}_HOST")
        database = os.getenv(f"{prefix}_DB")
        user = os.getenv(f"{prefix}_USER")
        password = os.getenv(f"{prefix}_PASSWORD")
        schema = os.getenv(f"{prefix}_SCHEMA")

        port_raw = os.getenv(f"{prefix}_PORT")
        port = int(port_raw) if port_raw else default_port

        if connector != ConnectorType.SQLITE and not all([host, database, user]):
            raise EnvironmentError(
                f"Variables manquantes pour le préfixe {prefix}_*: HOST, DB, USER sont requises"
            )

        return cls(
            name=source_name,
            connector=connector,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            schema=schema,
        )


@dataclass(frozen=True)
class KafkaClusterConfig:
    """Représente un cluster Kafka nommé."""

    name: str
    brokers: list[str]
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    topic_prefix: Optional[str] = None


@dataclass(frozen=True)
class IntegrationConfig:
    """Regroupe toutes les sources techniques (DB, Kafka) configurées pour le runtime."""

    databases: dict[str, RelationalSourceConfig] = field(default_factory=dict)
    kafka_clusters: dict[str, KafkaClusterConfig] = field(default_factory=dict)

    @classmethod
    def from_environment(cls, include_legacy: bool = True) -> "IntegrationConfig":
        """Construit la configuration en lisant les variables multi-sources de l'environnement."""
        databases: dict[str, RelationalSourceConfig] = {}
        kafka_clusters: dict[str, KafkaClusterConfig] = {}

        for source_name, fields in _extract_groups("ORCH_DB").items():
            connector_raw = fields.get("TYPE", "postgres").lower()
            connector = ConnectorType(connector_raw)
            port_raw = fields.get("PORT")
            port = int(port_raw) if port_raw else None

            params = _split_params(fields.get("PARAMS"))
            databases[source_name] = RelationalSourceConfig(
                name=source_name,
                connector=connector,
                host=fields.get("HOST"),
                port=port,
                database=fields.get("DB"),
                user=fields.get("USER"),
                password=fields.get("PASSWORD"),
                schema=fields.get("SCHEMA"),
                params=params,
            )

        for cluster_name, fields in _extract_groups("ORCH_KAFKA").items():
            brokers_raw = fields.get("BROKERS", "")
            brokers = [node.strip() for node in brokers_raw.split(",") if node.strip()]
            if not brokers:
                raise EnvironmentError(
                    f"Cluster Kafka '{cluster_name}' invalide: ORCH_KAFKA__{cluster_name.upper()}__BROKERS requis"
                )
            kafka_clusters[cluster_name] = KafkaClusterConfig(
                name=cluster_name,
                brokers=brokers,
                security_protocol=fields.get("SECURITY_PROTOCOL", "PLAINTEXT"),
                sasl_mechanism=fields.get("SASL_MECHANISM"),
                username=fields.get("USERNAME"),
                password=fields.get("PASSWORD"),
                topic_prefix=fields.get("TOPIC_PREFIX"),
            )

        if include_legacy and not databases:
            try:
                databases["dwh"] = RelationalSourceConfig.from_legacy_prefix(
                    prefix="DWH",
                    connector=ConnectorType.POSTGRES,
                    default_port=5432,
                )
            except EnvironmentError:
                pass

        return cls(databases=databases, kafka_clusters=kafka_clusters)

    def require_database(self, source_name: str) -> RelationalSourceConfig:
        """Retourne une source DB nommée ou lève une erreur explicite."""
        source_key = source_name.lower()
        source = self.databases.get(source_key)
        if source is None:
            raise KeyError(f"Source DB introuvable: {source_name}")
        return source


@dataclass(frozen=True)
class DatabaseConfig(RelationalSourceConfig):
    """Alias de compatibilité pour le code existant orienté PostgreSQL unique."""

    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        """Charge la source legacy `DWH_*` et la mappe vers le format historique."""
        source = RelationalSourceConfig.from_legacy_prefix(
            prefix="DWH",
            connector=ConnectorType.POSTGRES,
            default_port=5432,
        )
        return cls(
            name=source.name,
            connector=source.connector,
            host=source.host,
            port=source.port,
            database=source.database,
            user=source.user,
            password=source.password,
            schema=source.schema,
            params=source.params,
        )


def _extract_groups(namespace: str) -> dict[str, dict[str, str]]:
    """Extrait les groupes de variables au format `NAMESPACE__NAME__FIELD=VALUE`."""
    pattern = re.compile(rf"^{namespace}__([A-Z0-9_]+)__([A-Z0-9_]+)$")
    grouped: dict[str, dict[str, str]] = {}

    for key, value in os.environ.items():
        match = pattern.match(key)
        if not match:
            continue
        name = match.group(1).lower()
        field_name = match.group(2).upper()
        grouped.setdefault(name, {})[field_name] = value
    return grouped


def _split_params(raw_value: Optional[str]) -> dict[str, str]:
    """Convertit `k1=v1,k2=v2` vers un dictionnaire de paramètres de connexion."""
    if not raw_value:
        return {}

    params: dict[str, str] = {}
    for node in raw_value.split(","):
        chunk = node.strip()
        if not chunk:
            continue
        if "=" not in chunk:
            raise ValueError(f"Paramètre de connexion invalide (attendu key=value): {chunk}")
        key, value = chunk.split("=", 1)
        params[key.strip()] = value.strip()
    return params

