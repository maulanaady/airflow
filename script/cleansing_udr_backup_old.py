import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import timedelta, datetime
import os
import paramiko



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


def _checking_file(conn_id, **context):

    connection = BaseHook.get_connection(conn_id)
    host=connection.host
    username=connection.login
    password=connection.password
    port=connection.port

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    execution = context['data_interval_end'].date().strftime("%Y-%m-%d")
    command = f"""cd /tmpdir/mariabackup/zip && ls -alh | grep {execution}"""
    cmd = "sudo ps -ax | grep 'tar -zcvf /tmpdir/mariabackup/zip/'"

    try:
        if conn_id == 'ssh-cboss-snl' or conn_id == 'ssh-cboss-snt':
            key = '/opt/airflow/data/id_rsa'
        else:
            key = None

        ssh_client.connect(hostname=host, port=port,
                            username=username, password=password, key_filename=key)
        stdin, stdout, stderr = ssh_client.exec_command(command) 
        line = stdout.readlines()

        stdin, stdout, stderr = ssh_client.exec_command(cmd) 
        process = stdout.readlines()

        if len(line) > 0 and len(process) == 2:
            list_dev = []
            for i in range(len(line)):
                x = line[i].split()
                list_dev.append(x)

            current = datetime.now()+timedelta(hours=7)
            time = list_dev[0][7]
            day =datetime.today().date().strftime("%Y-%m-%d") + ' ' + time
            file_time = datetime.strptime(day, "%Y-%m-%d %H:%M")

            diff = (current - file_time).total_seconds() / 60
            if diff > 30:
                result ='next_task'
                filename = list_dev[0][8]

            else:
                result = 'skip_task'
                filename = None

        else:
            result = 'skip_task'
            filename = None

    except Exception as error:
        print(str(error))
        result = 'skip_task'
        filename = None        
    
    finally:
        context["task_instance"].xcom_push(key='checking_file', value=result)
        context["task_instance"].xcom_push(key='filename', value=filename)


    
def _pick_flow(**context):
    result = context["task_instance"].xcom_pull(task_ids='checking_file', key='checking_file')

    if result == 'next_task':
        return("copy_file")
    else:
        return("skip_task")

def _copy_file(conn_id, **context):

    connection = BaseHook.get_connection(conn_id)
    host=connection.host
    username=connection.login
    password=connection.password
    port=connection.port

    cnx = BaseHook.get_connection('ssh-analysis')
    pwd=cnx.password
 
    file = context["task_instance"].xcom_pull(task_ids='checking_file', key='filename')

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    command = f"""sudo sshpass -p "{pwd}" scp ubuntu@192.168.41.7:/tmpdir/mariabackup/zip/{file} /mnt/cephfs-nfs/bigdata/backup_test/zip"""
    try:

        if conn_id == 'ssh-cboss-snl' or conn_id == 'ssh-cboss-snt':
            key = '/opt/airflow/data/id_rsa'
        else:
            key = None

        ssh_client.connect(hostname=host, port=port,
                            username=username, password=password, key_filename=key)

        stdin, stdout, stderr = ssh_client.exec_command(command) 
        line = stdout.readlines()

        if len(line) > 0:
            list_dev = []
            for i in range(len(line)):
                x = line[i].split()
                list_dev.append(x)

            print(list_dev)

    except Exception as error:
        print(str(error))
    
def _check_copy_result(conn_id, **context):

    connection = BaseHook.get_connection(conn_id)
    host=connection.host
    username=connection.login
    password=connection.password
    port=connection.port

    file = context["task_instance"].xcom_pull(task_ids='checking_file', key='filename')

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    command = f"""cd /mnt/cephfs-nfs/bigdata/backup_test/zip && ls -alh | grep {file}"""

    try:

        if conn_id == 'ssh-cboss-snl' or conn_id == 'ssh-cboss-snt':
            key = '/opt/airflow/data/id_rsa'
        else:
            key = None

        ssh_client.connect(hostname=host, port=port,
                            username=username, password=password, key_filename=key)
        stdin, stdout, stderr = ssh_client.exec_command(command) 
        line = stdout.readlines()

        if len(line) > 0:
            result = 'delete_task'
        else:
            result = 'notif_error_copying_file'

    except Exception as error:
        print(str(error))
        result = 'notif_error_copying_file'
    finally:
        context["task_instance"].xcom_push(key='check_copy_result', value=result)    

def _pick_flow_result(**context):
    result = context["task_instance"].xcom_pull(task_ids='check_copy_result', key='check_copy_result')

    if result == 'delete_task':
        return("delete_file")
    else:
        return("notif_error_copying_file")

def _delete_file(conn_id, **context):

    connection = BaseHook.get_connection(conn_id)
    host=connection.host
    username=connection.login
    password=connection.password
    port=connection.port
 
    file = context["task_instance"].xcom_pull(task_ids='checking_file', key='filename')

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    date = context['data_interval_end'].date().strftime("%Y-%m-%d")

    command = f"""cd /tmpdir/mariabackup/zip && sudo rm -f {file} """
    command2 = f"sudo rm -rf /tmpdir/mariabackup/base/{date}"
    try:

        if conn_id == 'ssh-cboss-snl' or conn_id == 'ssh-cboss-snt':
            key = '/opt/airflow/data/id_rsa'
        else:
            key = None

        ssh_client.connect(hostname=host, port=port,
                            username=username, password=password, key_filename=key)

        ssh_client.exec_command(command)
        ssh_client.exec_command(command2) 

    except Exception as error:
        print(str(error))


def notif_error_copying_file(context):
    message = {
        'text': """<b>STATUS</b>: <b>TASK copy_file IS FAILED</b>
	<b>dag_id</b>: <b>{}</b>
	<b>Run</b>: <b>{}</b>
	<b>Task</b>: <b>{}</b>
	<b>Execution time</b>: <b>{}</b>""".format({context['dag_run'].dag_id}, {context['dag_run'].run_id}, {context['task'].task_id}, (context['next_execution_date']+timedelta(hours=7)).strftime("%Y-%m-%d, %H:%M:%S")),
        "disable_web_page_preview": True,
    }
    telegram_hook = TelegramHook(
        token='6227308181:AAFCn2EMtAJsAq5ocOp2B39jAjubbVLX2R4', chat_id=1012087010)
    telegram_hook.send_message(message)

dag = DAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    on_failure_callback = dag_failure_alert,
    schedule="@hourly",
    max_active_runs=1
)


checking_file = PythonOperator(
    task_id = 'checking_file',
    python_callable = _checking_file,
    op_kwargs={'conn_id':'ssh-analysis'},
    dag=dag
)


pick_flow = BranchPythonOperator(
    task_id = 'pick_flow',
    python_callable = _pick_flow,
    dag=dag
)


copy_file = PythonOperator(
    task_id = 'copy_file',
    python_callable = _copy_file,
    op_kwargs={'conn_id':'ssh-nfs'},
    dag=dag
)

check_copy_result = PythonOperator(
    task_id = 'check_copy_result',
    python_callable = _check_copy_result,
    op_kwargs={'conn_id':'ssh-nfs'},
    dag=dag
)

pick_flow_copy_result = BranchPythonOperator(
    task_id = 'pick_flow_result',
    python_callable = _pick_flow_result,
    dag=dag
)

delete_file = PythonOperator(
    task_id = 'delete_file',
    python_callable = _delete_file,
    op_kwargs={'conn_id':'ssh-analysis'},
    dag=dag
)

notif_error_copying_file = PythonOperator(
    task_id = 'notif_error_copying_file',
    python_callable = notif_error_copying_file,
    dag=dag
)


skip_task = EmptyOperator(
    task_id = 'skip_task',
    dag=dag
)


checking_file >> pick_flow >> [copy_file, skip_task]
copy_file >> check_copy_result >> pick_flow_copy_result >> [notif_error_copying_file, delete_file]
