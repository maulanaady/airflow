from typing import TYPE_CHECKING, Dict
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import telegram
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.hooks.base import BaseHook
import mysql.connector
from mysql.connector import errorcode
from airflow.exceptions import AirflowSkipException





def _send_telegram_alert_dag(**context):
    message = {
        'text':"""<b>STATUS</b>: <b>DAG/TASK IS FAILED</b>
	<b>dag_id</b>: <b>{}</b>
	<b>Run</b>: <b>{}</b>
	<b>Task</b>: <b>{}</b>
	<b>Execution time</b>: <b>{}</b>""".format({context['dag_run'].dag_id},{context['dag_run'].run_id},{context['task'].task_id},(context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    connection = BaseHook.get_connection("telegram")
    telegram_hook = TelegramHook(token=connection.password,chat_id=connection.host)
    telegram_hook.send_message(message)

def _send_telegram_message_check(conn_id):
    if conn_id != 'cboss-galera':
        mention = 'Hi <a href="tg://user?id=1012087010">Ady</a>, <a href="tg://user?id=119756720">Sahlan</a>, this message generated if database is not cboss-galera.\n'
    else:
        mention = ''
    message = {
        'text':"""{}""".format(mention),
        "disable_web_page_preview": True,
        'disable_notification': False,
        'parse_mode': 'HTML'
    }
    connection = BaseHook.get_connection("telegram")
    telegram_hook = TelegramHook(token='1613558114:AAHSoUmYdeX2NMwGAIpJTrryVZ7YjbTSEIY',chat_id=-567130388)
    result = telegram_hook.send_message(message)
    print('cek')
    print(dir(result))

dag = DAG(
    dag_id="test_message_telegram",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='*/30 * * * *',
    catchup=False,
)
 
for database in ['cboss-galera','cboss-snt']:
    telegram_notification = PythonOperator(
        task_id=f"send_telegram_message_check_{database}",
        python_callable=_send_telegram_message_check,
        op_kwargs={"conn_id": f"{database}"},
        dag=dag
        )



    telegram_notification





