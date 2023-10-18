import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
from datetime import timedelta
import pendulum

default_args = {
    'owner': 'default_user',
    # 'start_date': pendulum.datetime(2023, 1, 1, tz='Asia/Jakarta'),
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    # With this set to true, the pipeline won't run if the previous day failed
    'email': ['info@example.com'],
    'email_on_failure': True,
    # upon failure this pipeline will send an email to your email set above
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'simple_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


def my_func():
    print('Hello from my_func')


bashtask = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

dummy_task = DummyOperator(
    task_id='dummy_task',
    retries=3,
    dag=dag)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=my_func,
    dag=dag)

# python_task dependencies ke bashtask,
# python_task akan di execute jika bashtask success
bashtask.set_downstream(python_task)

# sama seperti diatas, dummy_task akan di execute jika bashtask success
dummy_task.set_upstream(bashtask)
