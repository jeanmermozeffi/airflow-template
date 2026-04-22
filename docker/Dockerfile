FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow
COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins
COPY src /opt/airflow/src
COPY scripts /opt/airflow/scripts
COPY config /opt/airflow/config
COPY monitoring /opt/airflow/monitoring
COPY docs /opt/airflow/docs
