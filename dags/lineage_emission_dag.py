"""Lineage Emission

This example demonstrates how to emit lineage to DataHub within an Airflow DAG.
"""

from datetime import timedelta

import datahub.emitter.mce_builder as builder
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}


with DAG(
    "datahub_lineage_emission_example",
    default_args=default_args,
    description="An example DAG demonstrating lineage emission within an Airflow DAG.",
    schedule=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
    default_view="tree",
) as dag:
    # This example shows a SnowflakeOperator followed by a lineage emission. However, the
    # same DatahubEmitterOperator can be used to emit lineage in any context.

    sql = """CREATE OR REPLACE TABLE psndba.test_datahub AS
            with table_a as
            (select 
                cpr_trx_subscriber_id,
                TRN_NO,
                TRN_DATE,
                WSC_TRX_PAYMENT_ID
            from bb_customer.cpr_trx_saldo
            where TRN_DATE >= STR_TO_DATE('2023-08-01 00:00:00','%Y-%m-%d %H:%i:%s')
            and TRN_DATE <= STR_TO_DATE('2023-08-10 00:00:00','%Y-%m-%d %H:%i:%s')
            ),
            table_b as
            (
                select
                PAYMENT_DATE,
                ID
            from bb_website.wsc_trx_payment
            )

            select *
            from table_a a
            left join table_b b on a.WSC_TRX_PAYMENT_ID = b.ID"""
    transformation_task = MySqlOperator(
        task_id="mariadb_transformation",
        dag=dag,
        mysql_conn_id="cboss-galera",
        sql=sql,
    )

    emit_lineage_task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_conn_id="datahub_rest_default",
        mces=[
            builder.make_lineage_mce(
                upstream_urns=[
                    builder.make_dataset_urn(
                        platform="mariadb", name="bb_customer.cpr_trx_saldo",
                        # platform_instance="dev_customer",
                    ),
                    builder.make_dataset_urn(
                        platform="mariadb",
                        name="bb_website.wsc_trx_payment",
                        # platform_instance="dev_website",
                        
                    ),
                ],
                downstream_urn=builder.make_dataset_urn(
                    platform="mariadb", name="psndba.test_datahub"
                ),
            )
        ],
    )

    transformation_task >> emit_lineage_task
