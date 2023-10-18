import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
import os
import paramiko


def on_failure(**context):
    ti = context['task_instance']
    print(f"task {ti.task_id } failed in dag { ti.dag_id } ")


with DAG(
    dag_id="monitoring_disk_usage_v2",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
    on_failure_callback=on_failure,
    catchup=False,
) as dag:

    @task
    def sshServer(conn_id, **context):

        hostname = conn_id[4:20].upper()
        if conn_id == 'ssh-backupdb':
            cmd = """df -h | grep /backup"""
        else:
            cmd = """df -h | grep /dev/sda1"""

        result = {}
        connection = BaseHook.get_connection(conn_id)

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:

            if conn_id == 'ssh-cboss-snl' or conn_id == 'ssh-cboss-snt':
                key = '/opt/airflow/data/id_rsa'
            else:
                key = None

            ssh_client.connect(hostname=connection.host, port=connection.port,
                               username=connection.login, password=connection.password, key_filename=key)
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            x = stdout.readlines()
            z = (x[0].split())
            size = z[4].replace("%", "")
            result['size'] = size
            result['dev'] = x[0]
            result['host'] = hostname+' - ' + str(connection.host)
            return (result)

        except paramiko.AuthenticationException as error:
            print("can't connect to ssh server")

    @task
    def sendTelegram(result, **context):
        value = int(result['size'])
        dev = result['dev']
        host = result['host']

        if value > 70:
            mention = f'<b>[{host}]</b>\nHi <a href="tg://user?id=905830939">Himmawan</a>, <a href="tg://user?id=42168291">Ririz</a>, please check following status :\nDisk partiition on  <b>root is {value} %</b>.\n\n{dev}'
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

    start = EmptyOperator(task_id='start', dag=dag)

    hostname = ['ssh-cboss-galera-01', 'ssh-cboss-galera-02',
                'ssh-cboss-galera-03', 'ssh-cboss-mhg', 'ssh-cboss-snt', 'ssh-cboss-snl', 'ssh-backupdb', 'ssh-analysis']
    SSHOperator = sshServer.expand(conn_id=hostname)
    sendTelegram = sendTelegram.expand(result=SSHOperator)

    start >> SSHOperator >> sendTelegram
