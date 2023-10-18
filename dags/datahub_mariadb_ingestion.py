"""MySQL DataHub Ingest DAG

This example demonstrates how to ingest metadata from MariaDB into DataHub
from within an Airflow DAG. Note that the DB connection configuration is
embedded within the code.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator


def ingest_from_mariadb():
    from datahub.ingestion.run.pipeline import Pipeline

    pipeline = Pipeline.create(
        # This configuration is analogous to a recipe configuration.
        {
            "source": {
                "type": "mariadb",
                "config": {
                    # If you want to use Airflow connections, take a look at the snowflake_sample_dag.py example.
                    "username": "cboss",
                    "password": "cboss2018",
                    "database": "psndba",
                    "host_port": "192.168.13.11:31047",
                    # "platform_instance": "dev",
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": "http://192.168.13.11:31246"},
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status()


with DAG(
    "datahub_mariadb_ingestion",
    default_args={
        "owner": "airflow",
    },
    description="An example DAG which ingests metadata from MariaDB to DataHub",
    start_date=datetime(2022, 1, 1),
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    default_view="tree",
) as dag:
    # While it is also possible to use the PythonOperator, we recommend using
    # the PythonVirtualenvOperator to ensure that there are no dependency
    # conflicts between DataHub and the rest of your Airflow environment.
    ingest_task = PythonVirtualenvOperator(
        task_id="ingest_from_mariadb",
        requirements=[
            "acryl-datahub[airflow]","pymysql"
        ],
        system_site_packages=False,
        python_callable=ingest_from_mariadb,
    )
