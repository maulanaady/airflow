import airflow
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from textwrap import dedent
import jinja2
import os

def dag_failure_alert(context):
    file = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
    mention = f"""Hi <a href="tg://user?id=1012087010">Ady</a>, there is an error at {file} job.\n"""
    message = {
        'text':"""{}""".format(mention),
        "disable_web_page_preview": True,
        'disable_notification': False,
        'parse_mode': 'HTML'
    }
    telegram_hook = TelegramHook(token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4',chat_id=1012087010)
    telegram_hook.send_message(message)


dag = DAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@hourly',
    on_failure_callback=dag_failure_alert, 
    max_active_runs=1,
    catchup=False,
    template_undefined=jinja2.Undefined
)

templated_get_data_command = """
echo "Getting Configurations..."

server_src="{{params.server_src}}"
database_src="{{params.database_src}}"
user_src="{{params.user_src}}"
password_src="{{params.password_src}}"
server_dest="{{params.server_dest}}"
database_dest="{{params.database_dest}}"
user_dest="{{params.user_dest}}"
password_dest="{{params.password_dest}}"

echo "start {{ execution_date }}"
echo "end {{ next_execution_date }}"

echo "Finished getting configurations. Start to execute main task ..."
bash /opt/spark/bin/spark-submit /home/node1/APACHE_SPARK/bb_useradmin/get__bb_useradmin__adm_trx_log_user_access.py {{ execution_date }} {{ next_execution_date }} ${server_src} ${database_src} ${user_src} ${password_src} ${server_dest} ${database_dest} ${user_dest} ${password_dest}
"""




templated_delete_data_command = """
echo "Getting Configurations..."

server_src="{{params.server_src}}"
database_src="{{params.database_src}}"
user_src="{{params.user_src}}"
password_src="{{params.password_src}}"
echo "Finished getting configurations. Start to execute main task (deleting data)..."
/home/node1/.local/bin/papermill /home/node1/APACHE_SPARK/bb_useradmin/delete__source__bb_useradmin__adm_trx_log_user_access.ipynb /home/node1/APACHE_SPARK/bb_useradmin/delete__source__bb_useradmin__adm_trx_log_user_access_out.ipynb -p start_date {{ data_interval_start }} -p end_date {{ data_interval_end }} -p server_source ${server_src} -p database_source ${database_src} -p user_source ${user_src} -p password_source ${password_src}

"""



start = DummyOperator(task_id='start', dag=dag)

get_data = SSHOperator(
    ssh_conn_id='ssh_jupyter',
    task_id='get_bb_useradmin_adm_trx_log_user_access',
    do_xcom_push = True,
    cmd_timeout = 180,
    command=templated_get_data_command,
    params = {"server_src": BaseHook.get_connection('mariadb_1316').host,
    "database_src": "bb_useradmin",
    "user_src": BaseHook.get_connection('mariadb_1316').login,
    "password_src": BaseHook.get_connection('mariadb_1316').password,
    "server_dest": BaseHook.get_connection('test_monitoring').host,
    "database_dest": "bb_useradmin",
    "user_dest": BaseHook.get_connection('test_monitoring').login,
    "password_dest": BaseHook.get_connection('test_monitoring').password},
    dag=dag)

#delete_data = SSHOperator(
#    ssh_conn_id='ssh_jupyter',
#    task_id='delete_bb_useradmin_adm_trx_log_user_access',
#    do_xcom_push = True,
#    cmd_timeout = 120,
#    command=templated_delete_data_command,
#    params = {"server_src": BaseHook.get_connection('test_monitoring').host,
#    "database_src": "bb_useradmin",
#    "user_src": BaseHook.get_connection('test_monitoring').login,
#    "password_src": BaseHook.get_connection('test_monitoring').password},
#    dag=dag)


start >> get_data 
#>> delete_data