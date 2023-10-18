import airflow
from airflow import DAG
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task
import mysql.connector
from airflow.datasets import Dataset


def dag_failure_alert(context):
    from datetime import timedelta
    message = {
        'text': """<b>STATUS</b>: <b>DAG/TASK IS FAILED</b>
	<b>dag_id</b>: <b>{}</b>
	<b>Run</b>: <b>{}</b>
	<b>Task</b>: <b>{}</b>
	<b>Execution time</b>: <b>{}</b>""".format({context['dag_run'].dag_id}, {context['dag_run'].run_id}, {context['task'].task_id}, (context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    telegram_hook = TelegramHook(
        token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4', chat_id=1012087010)
    telegram_hook.send_message(message)

example_dataset = Dataset("//monitoring")

with DAG(
    dag_id='monitoring_databases_status',
    start_date=airflow.utils.dates.days_ago(1),
    on_failure_callback=dag_failure_alert,
    tags=['monitoring'],
    max_active_runs=1,
    schedule='*/5 * * * *',
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start", outlets=[example_dataset])

    # trigger = TriggerDagRunOperator(
    #     task_id="trigger_dagrun",
    #     trigger_dag_id='galera_cluster_monitoring',
    # )

    @task
    def get_connection_status(conn_id, **context):
        from mysql.connector import errorcode
        result = {'db': conn_id, 'status': None, 'valid_': 'Y', 'IP': None}
        connection = BaseHook.get_connection(conn_id)
        try:
            conn = mysql.connector.connect(
                user=connection.login, password=connection.password, host=connection.host, database='information_schema')
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
            # output['result'] = result
        return (result)

    @task
    def write_report(result, **context):
        from mysql.connector import errorcode
        try:
            conn = mysql.connector.connect(
                user='dba', password='dba2010', host='192.168.4.30', database='observer')
            if conn.is_connected():
                cursor = conn.cursor()
                query_insert = 'insert into db_status(db,status,valid_) values(%s,%s,%s);'
                value_insert = (result.get('db'), result.get(
                    'status'), result.get('valid_'))
                cursor.execute(query_insert, value_insert)
                cursor.close()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
        finally:
            if mysql.connector.connect(user='dba', password='dba2010', host='192.168.4.30', database='observer').is_connected():
                conn.close()

#    @task
#    def error_only(result, **context):
#        # report = context["task_instance"].xcom_pull(task_ids=task_ids, key=key)
#        menit = int((context['next_execution_date']+timedelta(hours=7)).strftime("%M"))
#        #if result['status'] == 'Connected' or (result['db'] == 'cboss-galera' and menit > 0):
#        if result['db'] == 'cboss-galera':
#            raise AirflowSkipException("All databases are up and running.")

#    @task.branch
#    def choose_branch(result, **context):
#        if result['status'] != 'Connected':
#            return 'skip_task'
#        else:
#            return 'send_telegram_alert'

    @task
    def send_telegram_alert(result, **context):
        from datetime import timedelta
        menit = int((context['next_execution_date'] +
                    timedelta(hours=7)).strftime("%M"))
        if (result['status'] != 'Connected'):
            if (result['db'] != 'cboss-galera') or (result['db'] == 'cboss-galera' and menit == 0):
                if result['db'] != 'cboss-galera':
                    mention = 'Hi <a href="tg://user?id=905830939">Himmawan</a>, <a href="tg://user?id=42168291">Ririz</a> please check following status.\n'
                else:
                    mention = ''
                message = {
                    'text': """{}<b>ALERT STATUS</b>:\n<i>{}</i>\nDatabase: [{}]-{}\nExecution time: {}""".format(mention, result['status'], result['db'], result['IP'], (context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
                    "disable_web_page_preview": True,
                }
                connection = BaseHook.get_connection("telegram")
                telegram_hook = TelegramHook(
                    token=connection.password, chat_id=connection.host)
                # telegram_hook = TelegramHook(token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4',chat_id=1012087010)
                telegram_hook.send_message(message)

    databases = ['cboss-galera', 'cboss-mahaga',
                 'cboss-snt', 'cboss-snl', 'analysis', 'cboss-psn']
    get_connection_statuses = get_connection_status.expand(conn_id=databases)
    write_reports = write_report.expand(result=get_connection_statuses)
#    choose_branches = choose_branch.expand(result=get_connection_statuses)
    send_telegram_alerts = send_telegram_alert.expand(
        result=get_connection_statuses)

    start >> get_connection_statuses >> write_reports
    get_connection_statuses >> send_telegram_alerts

