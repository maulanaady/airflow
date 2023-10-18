import airflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from mysql.connector import errorcode
from airflow.decorators import dag, task
import mysql.connector
from datetime import datetime,timedelta
import time,os,re


def dag_failure_alert(context):
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


def _insert_data(data):
    connection = BaseHook.get_connection('cboss-galera')
    try:
        conn = mysql.connector.connect(
            user=connection.login, password=connection.password, host=connection.host, database='bb_customer')
        if conn.is_connected():
            cursor = conn.cursor()
            query_insert = """insert into bb_customer.cpr_trx_saldo_test(ID,PAYMENT_METHOD,CPR_TRX_SUBSCRIBER_ID,SALDO_TYPE,	
            INVOICE_NUMBER,TRN_CODE,TRN_NO,TRN_DATE,AMOUNT,KD_DB_KR,END_BALANCE,DESCRIPTION,DATE_CREATED,CREATED_BY,DATE_MODIFIED,
            MODIFIED_BY,ADM_MST_SITE_ID,REMARKS,CURRENCY,REFF_NUMBER,ACC_PAYMENT_GATEWAY,WSC_TRX_PAYMENT_ID,VOU_TRX_WIBO_VOUCHER_ID) 
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
            print('execute insert ..')
            print(data)
            cursor.executemany(query_insert, data)
            cursor.close()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(str(err))
    finally:
        if conn.is_connected():
            conn.commit()
            conn.close()

def _copy_data(conn_id, **context):
    start0 = context['data_interval_start']
    end0 = context['data_interval_end']
    print(start0)
    print(end0)
    
    start = context['data_interval_start'].strftime("%Y-%m-%d, %H:%M:%S")
    end = context['data_interval_end'].strftime("%Y-%m-%d, %H:%M:%S")

    rep = {"T": " ", "+00:00": ".000"} # define desired replacements here
    rep = dict((re.escape(k), v) for k, v in rep.items()) 
    pattern = re.compile("|".join(rep.keys()))

    start_date = pattern.sub(lambda m: rep[re.escape(m.group(0))], start)
    end_date = pattern.sub(lambda m: rep[re.escape(m.group(0))], end)

    final_start_date = datetime.strptime(start_date, "%Y-%m-%d, %H:%M:%S")+timedelta(days=-7)
    final_end_date = datetime.strptime(end_date, "%Y-%m-%d, %H:%M:%S")+timedelta(days=-7)

    connection = BaseHook.get_connection(conn_id)
    try:
        conn = mysql.connector.connect(
            user=connection.login, password=connection.password, host=connection.host, database='bb_customer')
        cursor = conn.cursor()
        sql = """
        select * from bb_customer.cpr_trx_saldo 
        where DATE_MODIFIED >= STR_TO_DATE('{}','%Y-%m-%d %H:%i:%S') and DATE_MODIFIED < STR_TO_DATE('{}','%Y-%m-%d %H:%i:%S')
        """.format(final_start_date, final_end_date)
        print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        print('start to insert')
        _insert_data(data=data)
        cursor.close()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Something is wrong with your user name or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist')
        else:
            print(str(err))
    finally:
        if mysql.connector.errors.InterfaceError:
            None
        else:
            conn.close()
            # conn.commit()



with DAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=airflow.utils.dates.days_ago(1),
    on_failure_callback=dag_failure_alert,
    schedule_interval='*/10 * * * *',
    max_active_runs=1,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')

    copy_data = PythonOperator(
        task_id="_copy_data",
	python_callable=_copy_data,
	op_kwargs={"conn_id": "cboss-galera"},
	dag=dag)







    start >> copy_data 
