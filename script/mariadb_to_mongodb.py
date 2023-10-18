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
    schedule_interval=None,
    on_failure_callback=dag_failure_alert, 
    max_active_runs=1,
    catchup=False,
    template_undefined=jinja2.Undefined
)

templated_get_data_command = """
echo "Getting Configurations..."

server_src='{{params.server_src}}'
user_src='{{params.user_src}}'
password_src='{{params.password_src}}'
database_src='{{params.database_src}}'
table_src='{{params.table_src}}'
min_id='{{params.min_id}}'
max_id='{{params.max_id}}'
inc='{{params.inc}}'
server_dest='{{params.server_dest}}'
database_dest='{{params.database_dest}}'
collection_dest='{{params.collection_dest}}'
user_dest='{{params.user_dest}}'
password_dest='{{params.password_dest}}'
log_path='{{params.log_path}}'
apps_name='{{params.apps_name}}'


echo "start {{ execution_date }}"
echo "end {{ next_execution_date }}"
echo "========================="
echo ${server_src}
echo ${database_src}
echo ${table_src}
echo ${user_src}
echo ${password_src}
echo ${server_dest}
echo ${database_dest}
echo ${collection_dest}
echo ${user_dest}
echo ${password_dest}
echo ${log_path}
echo ${apps_name}

echo "Finished getting configurations. Start to execute main task ..."
python3 /home/node1/APACHE_SPARK/bb_customer/mariadb_to_mongodb_initial_load.py ${server_src} ${user_src} ${password_src} ${database_src} ${table_src} ${min_id} ${max_id} ${inc} ${server_dest} ${user_dest} ${password_dest} ${database_dest} ${collection_dest} ${log_path} ${apps_name}

"""
#mariadb_to_mongodb_initial_load.py ${server_src} ${user_src} ${password_src} ${database_src} ${table_src} ${min_id} ${max_id} ${inc} ${server_dest} ${user_dest} ${password_dest} ${database_dest} ${collection_dest} ${log_path} ${apps_name}

start = DummyOperator(task_id='start', dag=dag)

get_cpr_trx_saldo = SSHOperator(
    ssh_conn_id='ssh_jupyter',
    task_id='get_bb_customer_cpr_trx_saldo',
    do_xcom_push = True,
    cmd_timeout = 600,
    command=templated_get_data_command,
    params = {"server_src": BaseHook.get_connection('cboss-galera').host,
    "user_src": BaseHook.get_connection('cboss-galera').login,
    "password_src": BaseHook.get_connection('cboss-galera').password,
    "database_src": "bb_customer",
    "table_src": "cpr_trx_saldo",
    "min_id": 0,
    "max_id": 1000000,
    "inc": 1000,
    "server_dest": BaseHook.get_connection('mongo_test').host,
    "user_dest": BaseHook.get_connection('mongo_test').login,
    "password_dest": BaseHook.get_connection('mongo_test').password,
    "database_dest": "bb_customer",
    "collection_dest": "cpr_trx_Saldo",
    "log_path": "/home/node1/APACHE_SPARK/apps_log",
    "apps_name": "mariadb_to_mongodb_cpr_trx_saldo"},
    dag=dag)

 



start >> get_cpr_trx_saldo