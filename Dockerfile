FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root dbt_project/ /opt/airflow/dbt_project/
COPY --chown=airflow:root extractors/ /opt/airflow/extractors/
COPY --chown=airflow:root loaders/ /opt/airflow/loaders/
COPY --chown=airflow:root config/ /opt/airflow/config/

WORKDIR /opt/airflow
