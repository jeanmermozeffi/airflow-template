from orchestration.common.config import IntegrationConfig


def build_kafka_client_config(cluster_name: str, integration_config: IntegrationConfig | None = None) -> dict:
    """Construit une config client Kafka générique à partir d'un cluster nommé."""
    config = integration_config or IntegrationConfig.from_environment()
    cluster = config.kafka_clusters.get(cluster_name.lower())
    if cluster is None:
        raise KeyError(f"Cluster Kafka introuvable: {cluster_name}")

    client_config = {
        "bootstrap.servers": ",".join(cluster.brokers),
        "security.protocol": cluster.security_protocol,
    }

    if cluster.sasl_mechanism:
        client_config["sasl.mechanism"] = cluster.sasl_mechanism
    if cluster.username:
        client_config["sasl.username"] = cluster.username
    if cluster.password:
        client_config["sasl.password"] = cluster.password

    return client_config

