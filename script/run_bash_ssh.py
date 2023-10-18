import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator



dag = DAG(
    dag_id="call_remote_script",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
)


start = DummyOperator(task_id='start', dag=dag)

t1_bash = """
echo 'start running script'
df -h
"""
t1 = SSHOperator(
    ssh_conn_id='tableau_server',
    task_id='test_ssh_operator',
    cmd_timeout = 60,
    command=t1_bash,
    dag=dag)

start >> t1