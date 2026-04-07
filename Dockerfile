FROM astrocrpublic.azurecr.io/runtime:3.1-14

RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core==1.9.* \
    dbt-snowflake==1.9.*