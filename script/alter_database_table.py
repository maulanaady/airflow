from datetime import datetime

from airflow.decorators import task
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import mysql.connector
from mysql.connector import errorcode
import csv, time

with DAG(
    dag_id="alter_database_tables",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={"owner": "donus", "start_date": "2021-08-15"},
) as dag:

    task1 = EmptyOperator(task_id="validation_1")

    @task()
    def execute_alter(sql):
        try:
            conn = mysql.connector.connect(user='cboss', password='cboss2018',host='192.168.41.227',database='bb_useradmin')
            conn.raise_on_warnings = True
            #conn.get_warnings = True
            if conn.is_connected():
                cursor = conn.cursor()
                print(f'start execute sql: {sql}')
                cursor.execute(sql)
                #time.sleep(10)
                data = cursor.fetchall()
                print('cek')
                print(data)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
        finally:
            if mysql.connector.connect(user='cboss', password='cboss2018',host='192.168.41.227',database='bb_useradmin'):
                cursor.close()
                conn.close()

    list_tasks_get_results = ['alter table bb_api.api_mst_resources MODIFY COLUMN ID BIGINT(20) auto_increment NOT NULL',
        'alter table bb_api.api_trx_post_body_uri MODIFY COLUMN ID BIGINT(20) auto_increment NOT NULL'
    ]
    get_results = execute_alter.expand(sql=list_tasks_get_results)

    task1 >> get_results