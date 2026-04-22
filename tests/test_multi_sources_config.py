from orchestration.common.config import ConnectorType, IntegrationConfig


def test_parse_multiple_databases_of_same_connector(monkeypatch) -> None:
    """Vérifie la prise en charge de plusieurs sources PostgreSQL distinctes."""
    monkeypatch.setenv("ORCH_DB__DWH__TYPE", "postgres")
    monkeypatch.setenv("ORCH_DB__DWH__HOST", "pg-host")
    monkeypatch.setenv("ORCH_DB__DWH__PORT", "5432")
    monkeypatch.setenv("ORCH_DB__DWH__DB", "dwh")
    monkeypatch.setenv("ORCH_DB__DWH__USER", "dwh_user")
    monkeypatch.setenv("ORCH_DB__DWH__PASSWORD", "dwh_pwd")

    monkeypatch.setenv("ORCH_DB__RAW__TYPE", "postgres")
    monkeypatch.setenv("ORCH_DB__RAW__HOST", "pg-host")
    monkeypatch.setenv("ORCH_DB__RAW__PORT", "5432")
    monkeypatch.setenv("ORCH_DB__RAW__DB", "raw")
    monkeypatch.setenv("ORCH_DB__RAW__USER", "raw_user")
    monkeypatch.setenv("ORCH_DB__RAW__PASSWORD", "raw_pwd")

    config = IntegrationConfig.from_environment(include_legacy=False)

    assert set(config.databases.keys()) == {"dwh", "raw"}
    assert config.databases["dwh"].connector == ConnectorType.POSTGRES
    assert config.databases["raw"].connector == ConnectorType.POSTGRES


def test_parse_kafka_cluster(monkeypatch) -> None:
    """Vérifie la prise en charge des clusters Kafka nommés."""
    monkeypatch.setenv("ORCH_KAFKA__MAIN__BROKERS", "kafka1:9092,kafka2:9092")
    monkeypatch.setenv("ORCH_KAFKA__MAIN__SECURITY_PROTOCOL", "PLAINTEXT")

    config = IntegrationConfig.from_environment(include_legacy=False)
    assert "main" in config.kafka_clusters
    assert config.kafka_clusters["main"].brokers == ["kafka1:9092", "kafka2:9092"]

