import airflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import os
import logging
from datetime import timedelta
import mysql.connector
from mysql.connector import errorcode


def _export_to_csv(conn_id, **context):
    print("***************************************************")
    print((context['execution_date']).strftime("%Y-%m-%d %H:%M:%S"))
    print("***************************************************")
    print(context['next_execution_date'].strftime("%Y-%m-%d %H:%M:%S"))
    print("***************************************************")

    ts = (context['execution_date']+timedelta(hours=7)
          ).strftime("%Y-%m-%d %H:%M:%S")
    next_ts = (context['next_execution_date'] +
               timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
#    date_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = "/opt/airflow/data/RAW"
    file = "ADM_TRX_LOG"+ts+'_'+next_ts+".csv"
    path_file = path+file

    logger = logging.getLogger(__name__)

    connection = BaseHook.get_connection(conn_id)

    try:
        conn = mysql.connector.connect(user=connection.login,
                                       password=connection.password,
                                       host=connection.host,
                                       database='bb_useradmin')
        if conn.is_connected():
            logger.info(
                " ===================== Database is connected =====================")
            sql = f"""select * from adm_trx_log_user_access where DATE_CREATED between '{ts}' and '{next_ts}';"""
            header = ['ID', 'ADM_MST_USER_ID', 'IP_ADDRESS', 'USERAGENT', 'BROWSER_TYPE', 'BROWSER_VERSION', 'OS_TYPE', 'OS_VERSION',
                      'APP_TYPE', 'REMARKS', 'DATE_CREATED', 'CREATED_BY', 'CPR_TRX_SUBSCRIBER_ID', 'URL', 'SAPI_NAME', 'EMP_ID', 'ACCESSED_FROM']

            cursor = conn.cursor()
            cursor.execute(sql)
            data = cursor.fetchall()
            cursor.close()
            print(f"ts : {ts}")
            print(f"next_ts : {next_ts}")

            with open(path_file, 'w', newline='') as f:
                write = csv.writer(f, lineterminator=os.linesep)
                write.writerow(header)
                write.writerows(data)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.info('Something is wrong with your user name or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.info('Database does not exist')
        else:
            logger.info(str(err))
    finally:
        if mysql.connector.errors.InterfaceError:
            None
        else:
            conn.close()


dag = DAG(
    dag_id="mysql_export_to_csv_file",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='0 */1 * * *',
    catchup=False,
)

export_file = PythonOperator(
    task_id='export_mysql_to_csv',
    python_callable=_export_to_csv,
    op_kwargs={'conn_id': 'mariadb_1316'},
    dag=dag

)

export_file
