"""
test
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
"""
import os
from datetime import timedelta
import pendulum
import airflow
import jinja2
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': "operations",
    'depends_on_past': False,
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': pendulum.datetime(2023, 5, 1, tz="Asia/Jakarta"),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id = os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2023, 5, 1, tz="Asia/Jakarta"),
    catchup = False,
    tags=['airflow-maintenance-dags'],
    template_undefined=jinja2.Undefined
) as dag:
    
    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__
    if hasattr(dag, 'catchup'):
        dag.catchup = False

    start = DummyOperator(
        task_id='start',
        dag=dag)

    log_cleanup = """
    echo "Configurations:"
    echo "DATA_INTERVAL_START:      '${DATA_INTERVAL_START}'"
    test=$(date -d ${DATA_INTERVAL_START})
    test2=$(date -d "$test - 41 hours" +"%FT%H:%M:%S")

    echo "Running Cleanup Process..."
    /home/airflow/.local/bin/airflow db clean --clean-before-timestamp ${test2} --skip-archive -t 'log','xcom','dag_run','task_instance','sla_miss','dag_run','task_reschedule','task_fail','import_error','task_instance','celery_taskmeta','dataset_event','job','session','celery_tasksetmeta' --yes
    #'rendered_task_instance_fields'
    echo "Finished Running Cleanup Process"

    """

    date = "{{ ts }}"
    db_cleanup_op = BashOperator(
        task_id='db_cleanup',
        bash_command=log_cleanup,
        env={"DATA_INTERVAL_START": date},
        dag=dag)

    db_cleanup_op.set_upstream(start)

