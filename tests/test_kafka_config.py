from orchestration.common.config import IntegrationConfig
from orchestration.kafka.client_config import build_kafka_client_config


def test_build_kafka_client_config(monkeypatch) -> None:
    """Vérifie la construction de la config client Kafka depuis l'environnement."""
    monkeypatch.setenv("ORCH_KAFKA__MAIN__BROKERS", "kafka1:9092,kafka2:9092")
    monkeypatch.setenv("ORCH_KAFKA__MAIN__SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("ORCH_KAFKA__MAIN__SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("ORCH_KAFKA__MAIN__USERNAME", "k_user")
    monkeypatch.setenv("ORCH_KAFKA__MAIN__PASSWORD", "k_pwd")

    integration_config = IntegrationConfig.from_environment(include_legacy=False)
    client_config = build_kafka_client_config("main", integration_config=integration_config)

    assert client_config["bootstrap.servers"] == "kafka1:9092,kafka2:9092"
    assert client_config["security.protocol"] == "SASL_SSL"
    assert client_config["sasl.mechanism"] == "PLAIN"

