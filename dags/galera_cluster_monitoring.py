import airflow,os
from airflow import DAG
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from datetime import timedelta

def dag_success_alert(context):
    message = {
        'text': """<b>STATUS</b>: <b>DAG IS SUCCEED</b>
	<b>dag_id</b>: <b>{}</b>
	<b>Run</b>: <b>{}</b>
	<b>Execution time</b>: <b>{}</b>""".format({context['dag_run'].dag_id}, {context['dag_run'].run_id}, (context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    telegram_hook = TelegramHook(
        token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4', chat_id=1012087010)
    telegram_hook.send_message(message)

def dag_failure_alert(context):
    message = {
        'text': """<b>STATUS</b>: <b>DAG/TASK IS FAILED</b>
	<b>dag_id</b>: <b>{}</b>
	<b>Run</b>: <b>{}</b>
	<b>Execution time</b>: <b>{}</b>""".format({context['dag_run'].dag_id}, {context['dag_run'].run_id}, (context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    telegram_hook = TelegramHook(
        token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4', chat_id=1012087010)
    telegram_hook.send_message(message)


example_dataset = Dataset("//monitoring")

with DAG(
    dag_id='galera_cluster_monitoring',
    start_date=airflow.utils.dates.days_ago(1),
    schedule=[example_dataset],				# [example_dataset]
    on_success_callback=dag_success_alert,
    on_failure_callback=dag_failure_alert,
    tags=['monitoring'],
    max_active_runs=1,
    catchup=False,
) as dag:

    @task(queue='main_queue')
    def sshServer(conn_id: str, **context) -> list:
        import paramiko
        connection = BaseHook.get_connection(conn_id)
        host=connection.host
        username=connection.login
        password=connection.password
        port=connection.port
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if conn_id == 'ssh-cboss-snl' or conn_id == 'ssh-cboss-snt':
                key = '/opt/airflow/data/id_rsa'
            else:
                key = None

            ssh_client.connect(hostname=host, port=port,
                               username=username, password=password, key_filename=key)

            gemilang_cnx = BaseHook.get_connection('galera-gemilang')
            host = gemilang_cnx.host
            user = gemilang_cnx.login
            pwd = gemilang_cnx.password     
            cmd = f"""mysql -h {host} -u{user} -p{pwd} -e 'show status like "wsrep_incoming_addresses"'"""
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            gemilang_line = stdout.readlines()
            dict = {'GALERA-GEMILANG': gemilang_line}

            oss_cnx = BaseHook.get_connection('galera-oss-70G')
            oss_host = oss_cnx.host
            oss_user = oss_cnx.login
            oss_pwd = oss_cnx.password
            cmd = f"""mysql -h {oss_host} -u{oss_user} -p{oss_pwd} -e 'show status like "wsrep_incoming_addresses"'"""
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            oss_line = stdout.readlines()
            dict.update({'GALERA-OSS-70G': oss_line})
            return(dict)

        except paramiko.AuthenticationException as error:
            logging.info("can't connect to ssh server")
            return None

    @task()
    def send_telegram(dict):
        for i,list in dict.items():
            a = list[1].split('\t')
            b = a[1].replace("\n","").split(',')   
            if len(b) < 3:
                connection = BaseHook.get_connection("telegram")
                token=connection.password
                chat_id=connection.host
                message = {
                        'text': f'<b>ALERT STATUS: MONITORING {i}</b>\nSome node is down, current running node are:\n{b}',
                        "disable_web_page_preview": True,
                }
                telegram_hook = TelegramHook(token=token, chat_id=chat_id)
                telegram_hook.send_message(message)


    start = EmptyOperator(task_id='start', dag=dag, queue='main_queue')
    SSHOperator = sshServer('ssh-cboss-galera-03')
    telegram = send_telegram(SSHOperator)



    start >> SSHOperator >> telegram
