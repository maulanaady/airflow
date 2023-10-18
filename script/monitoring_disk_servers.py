import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.decorators import dag, task
import base64
from airflow.providers.telegram.hooks.telegram import TelegramHook


def on_failure_callback(**context):
    ti = context['task_instance']
    print(f"task {ti.task_id } failed in dag { ti.dag_id } ")


def _getDisk(task_id, key, **context):
    value = context["task_instance"].xcom_pull(task_ids=task_id, key=key)
    diskPartition = base64.b64decode(value).decode('utf-8')
    # print(value)
    key = "return_xcom"
    x = diskPartition.split()
    y = int(x[4].replace("%", ""))

    id = task_id[9:22]

    if y > 70:
        mention = f'<b>[Server {id}]</b>\nHi <a href="tg://user?id=905830939">Himmawan</a>, <a href="tg://user?id=42168291">Ririz</a> please check following status :\nDisk partiition root is <b>{y} %</b> .\n{diskPartition}'
        message = {
            'text': """{}""".format(mention),
            "disable_web_page_preview": True,
            'disable_notification': False,
            'parse_mode': 'HTML'
        }
        telegram_hook = TelegramHook("telegram")
        result = telegram_hook.send_message(message)
    else:
        print("Partition disk is still available")


dag = DAG(
    dag_id="monitoring-disk-usage",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
    on_failure_callback=on_failure_callback
)

start = EmptyOperator(task_id='start')
sshServer = ['ssh-cboss-mhg', 'ssh-cboss-snt', 'ssh-cboss-snl',
             'ssh-cboss-galera1', 'ssh-cboss-galera2', 'ssh-cboss-galera3']

for conn in sshServer:

    cmd = """df -h | grep /dev/sda1"""

    ssh_op = SSHOperator(
        ssh_conn_id=f"{conn}",
        task_id=f"task-{conn}",
        do_xcom_push=True,
        command=cmd,
        dag=dag
    )

    task2 = PythonOperator(
        task_id=f"cfdisk-{conn}",
        python_callable=_getDisk,
        op_kwargs={"task_id": f"task-{conn}", "key": "return_value"},
        dag=dag
    )

    ssh_op >> task2
