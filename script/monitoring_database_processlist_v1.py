import airflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from mysql.connector import errorcode
from airflow.decorators import dag, task
import mysql.connector
import logging
import csv
import os
import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
import time
import numpy as np


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


with DAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=airflow.utils.dates.days_ago(1),
    on_failure_callback = dag_failure_alert,
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    @task
    def get_processlist(conn_id,**context):
       logger = logging.getLogger(__name__)
       connection = BaseHook.get_connection(conn_id)
       try:
           conn = mysql.connector.connect(user=connection.login, password=connection.password,host=connection.host,database='information_schema')
           cursor = conn.cursor()
           cursor.execute("show processlist")
           data = cursor.fetchall()
           cursor.close()
           header = ['Id','User','Host','db','Command','Time','State','Info','Progress']
           with open(f"/opt/airflow/data/{conn_id}.csv", 'w',newline='') as f:
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
        
    @task
    def get_summary(conn_id, **context):
        ts = (context['data_interval_end']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")
        if os.path.exists(f"/opt/airflow/data/{conn_id}.csv"):
            data = pd.read_csv(f"/opt/airflow/data/{conn_id}.csv", delimiter=',')
            if not data.empty:
                summary = data[['User','Command']].groupby('User').count().reset_index()
                summary = summary[(summary['Command'] >= 50)]		

                if not summary.empty:
                    user = list(summary['User'])
                    count = list(summary['Command'])
                    fig = plt.figure(figsize = (10, 5))
                    plt.bar(user, count, fc="lightgrey",ec="black")

                    for i in range(len(user)):
                        plt.text(i,count[i],count[i],va="bottom")

                    plt.xlabel("User")
                    plt.ylabel("No. of Command")
                    plt.title(f"Monitoring Database {conn_id} Processlist at {ts}")
                    plt.savefig(f"/opt/airflow/data/{conn_id}.png")
                    del user, count, summary

                data_per_command = data[(data['Time'].fillna(0).astype(np.int32) > 6000) & ~(data['Command'].isin(['Sleep','Daemon']))]
                if not data_per_command.empty:
                    summary_per_command = data_per_command[['User','Command','Time']].groupby(['User','Command'])['Time'].sum().reset_index()
                    summary_per_command['x'] = summary_per_command['User'] + ' - ' +summary_per_command['Command']

                    user_per_command = list(summary_per_command['x'])
                    time_per_command = list(summary_per_command['Time'].astype(np.int32))
                    del summary_per_command, data_per_command

                    fig = plt.figure(figsize = (10, 5))
                    plt.bar(user_per_command, time_per_command, fc="lightgrey",ec="black")
                    for i in range(len(user_per_command)):
                        plt.text(i,time_per_command[i],time_per_command[i],va="bottom")

                    plt.xlabel("User-Command")
                    plt.ylabel("Cummulative Time (Seconds)")
                    plt.title(f"Monitoring Database {conn_id} Processlist per Command at {ts}")
                    plt.savefig(f"/opt/airflow/data/{conn_id}_per_command.png")


    @task
    def send_picture(conn_id):
        if os.path.exists(f"/opt/airflow/data/{conn_id}.png") or os.path.exists(f"/opt/airflow/data/{conn_id}_per_command.png"):
            connection = BaseHook.get_connection("telegram")
            token = connection.password
            chat_id = connection.host
            # token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4'
            # chat_id=1012087010

            list = os.listdir("/opt/airflow/data")
            for item in list:
                if item.endswith(f"{conn_id}_per_command.png") or item.endswith(f"{conn_id}.png"):
                    files = {
                    'photo': open(f"/opt/airflow/data/{item}", 'rb')
                    }
                    message = 'https://api.telegram.org/bot{}/sendPhoto?chat_id={}'.format(token, chat_id)
                    try:
                        send = requests.post(message, files = files)
                    except Exception as e:
                        logger.info(e)                    

    @task(trigger_rule = "none_failed")
    def delete_file(conn_id):
       list = os.listdir("/opt/airflow/data")
       for item in list:
           if item.endswith(f"{conn_id}.csv") or item.endswith(f"{conn_id}.png") or item.endswith(f"{conn_id}_per_command.png"):
               os.remove(os.path.join("/opt/airflow/data", item))                       

    databases = ['cboss-mahaga','cboss-snt','cboss-snl', 'analysis']
    get_processlists = get_processlist.expand(conn_id=databases)
    get_summaries = get_summary.expand(conn_id=databases)
    send_pictures = send_picture.expand(conn_id=databases)
    delete_files = delete_file.expand(conn_id=databases)


    start >> get_processlists >> get_summaries >> send_pictures >> delete_files
    # start >> send_pictures
    