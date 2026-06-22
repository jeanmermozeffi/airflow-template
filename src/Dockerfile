ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.1.5-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

# Dépendances Python additionnelles du projet
COPY requirements-docker.txt /opt/airflow/requirements-docker.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements-docker.txt

# Code source et scripts
COPY --chown=airflow:root orchestration/ /opt/airflow/orchestration/
COPY --chown=airflow:root scripts/       /opt/airflow/scripts/
COPY --chown=airflow:root config/        /opt/airflow/config/
COPY --chown=airflow:root dags/          /opt/airflow/dags/
COPY --chown=airflow:root plugins/       /opt/airflow/plugins/
COPY --chown=airflow:root include/       /opt/airflow/include/
COPY --chown=airflow:root .env           /opt/airflow/.env

USER root
RUN chmod 755 /opt/airflow/scripts/airflow_bootstrap_entrypoint.sh
USER airflow

ENV AIRFLOW_BOOTSTRAP_MODE=metadata \
    AIRFLOW_BOOTSTRAP_ON_START=true \
    AIRFLOW_BOOTSTRAP_WAIT_FOR_MIGRATIONS=true \
    AIRFLOW_ORIGINAL_ENTRYPOINT=/entrypoint

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/scripts/airflow_bootstrap_entrypoint.sh"]
