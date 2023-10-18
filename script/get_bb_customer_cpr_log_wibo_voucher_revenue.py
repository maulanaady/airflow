import airflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from textwrap import dedent
import jinja2

dag = DAG(
    dag_id="bb_customer_cpr_log_wibo_voucher_revenue",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
    template_undefined=jinja2.Undefined
)

templated_get_data_command = """
echo "Getting Configurations..."

delta_revenue_date="{{params.delta_revenue_date}}"

echo "Finished getting configurations. Start to execute main task (reading data)..."
/home/node1/.local/bin/papermill /home/node1/APACHE_SPARK/bb_customer/get__bb_customer__cpr_log_wibo_voucher_revenue.ipynb /home/node1/APACHE_SPARK/bb_customer/get__bb_customer__cpr_log_wibo_voucher_revenue_out.ipynb -p delta_revenue_date ${delta_revenue_date}
"""


start = DummyOperator(task_id='start', dag=dag)

get_data = SSHOperator(
    ssh_conn_id='ssh_jupyter',
    task_id='get_bb_customer_cpr_log_wibo_voucher_revenue',
    do_xcom_push = True,
    cmd_timeout = 120,
    command=templated_get_data_command,
    params = {"delta_revenue_date": -5},
    dag=dag)

start >> get_data