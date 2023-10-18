import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
#from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.hooks.base import BaseHook
import mysql.connector
from mysql.connector import errorcode
from airflow.exceptions import AirflowSkipException




def _get_connection_status(conn_id, key,**context):
    result = {'db':conn_id,'status':None,'valid_':'Y', 'IP': None}
    connection = BaseHook.get_connection(conn_id)
    try:
        conn = mysql.connector.connect(user=connection.login, password=connection.password,host=connection.host,database='information_schema')
        if conn.is_connected():
            status = 'Connected'
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          status = 'Something is wrong with your user name or password'
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          status = 'Database does not exist'
        else: 
          status = str(err)
    finally:
        if mysql.connector.errors.InterfaceError:
            None
        else:
            conn.close()
        result['status'] = status
        result['IP'] = connection.host

    context["task_instance"].xcom_push(key=key, value=result)


def _write_report(task_ids, key, **context):
    report = context["task_instance"].xcom_pull(task_ids=task_ids, key=key)
    try:
        conn = mysql.connector.connect(user='dba', password='dba2010',host='192.168.4.30',database='observer')
        if conn.is_connected():
            cursor = conn.cursor()
            query_insert = 'insert into db_status(db,status,valid_) values(%s,%s,%s);'
            value_insert = (report.get('db'),report.get('status'),report.get('valid_'))
            cursor.execute(query_insert, value_insert)
            cursor.close()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
    finally:
        if mysql.connector.connect(user='dba', password='dba2010',host='192.168.4.30',database='observer').is_connected():
            conn.close()

def _error_only(task_ids, key, **context):
    report = context["task_instance"].xcom_pull(task_ids=task_ids, key=key)
    menit = int((context['next_execution_date']+timedelta(hours=7)).strftime("%M"))
    if report['status'] == 'Connected' or (report['db'] == 'cboss-galera' and menit > 0):
         raise AirflowSkipException("All databases are up and running.")

def dag_failure_alert(context):
    message = {
        'text':"""<b>STATUS</b>: <b>DAG/TASK IS FAILED</b>
	<b>dag_id</b>: <b>{}</b>
	<b>Run</b>: <b>{}</b>
	<b>Task</b>: <b>{}</b>
	<b>Execution time</b>: <b>{}</b>""".format({context['dag_run'].dag_id},{context['dag_run'].run_id},{context['task'].task_id},(context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    telegram_hook = TelegramHook(token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4',chat_id=1012087010)
    telegram_hook.send_message(message)


def _send_telegram_alert(task_ids, key, **context):
    report = context["task_instance"].xcom_pull(task_ids=task_ids, key=key)
    if report['db'] != 'cboss-galera':
        mention = 'Hi <a href="tg://user?id=905830939">Himmawan</a>, <a href="tg://user?id=42168291">Ririz</a> please check following status.\n'
    else:
        mention = ''
    message = {
        'text':"""{}<b>ALERT STATUS</b>:
	<i>{}</i>
	Database: [{}]-{}
	Execution time: {}""".format(mention, report['status'], report['db'],report['IP'],(context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    connection = BaseHook.get_connection("telegram")
    telegram_hook = TelegramHook(token=connection.password,chat_id=connection.host)
    telegram_hook.send_message(message)

dag = DAG(
    dag_id="monitoring_databases_status",
    start_date=airflow.utils.dates.days_ago(1),
    on_failure_callback = dag_failure_alert,
    schedule_interval='*/5 * * * *',
    catchup=False,
)
 
for database in ['cboss-galera', 'cboss-mahaga','cboss-snt','cboss-snl','analysis']:
    get_connection_status = PythonOperator(
        task_id=f"get_connection_status_{database}",
	python_callable=_get_connection_status,
	op_kwargs={"conn_id": f"{database}", "key": f"status_{database}"},
	dag=dag
    )
    
    write_report = PythonOperator(
        task_id=f"write_report_{database}",
	python_callable=_write_report,
	op_kwargs={"task_ids": f"get_connection_status_{database}", "key": f"status_{database}"},
	dag=dag)


    error_report = PythonOperator(
        task_id=f"error_report_{database}",
	python_callable=_error_only,
	op_kwargs={"task_ids": f"get_connection_status_{database}", "key": f"status_{database}"},
	dag=dag
    )

    telegram_notification = PythonOperator(
        task_id=f"telegram_notification_{database}",
	python_callable=_send_telegram_alert,
	op_kwargs={"task_ids": f"get_connection_status_{database}", "key": f"status_{database}"},
	dag=dag
    )



    get_connection_status >> write_report
    get_connection_status >> error_report >> telegram_notification





