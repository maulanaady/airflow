import airflow, paramiko
from airflow import DAG
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task


def on_failure(**context):
    ti = context['task_instance']
    print(f"task {ti.task_id } failed in dag { ti.dag_id } ")


with DAG(
    dag_id="monitoring_disk_usage_v4",
    start_date=airflow.utils.dates.days_ago(1),
    schedule='@hourly',
    tags=['monitoring'],
    on_failure_callback=on_failure,
    max_active_runs=1,
    catchup=False,
) as dag:

    @task(queue='main_queue')
    def sshServer(conn_id, **context):

        hostname = conn_id[4:20].upper()
        
        

        result = {}
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
            stdin, stdout, stderr = ssh_client.exec_command("df -h")
            line = stdout.readlines()

            list_dev = []
            for i in range(len(line)):
                x = line[i].split()
                x.append(hostname+' - ' + host)
                list_dev.append(x)

            return list_dev

        except paramiko.AuthenticationException as error:
            print("can't connect to ssh server")

    @task(queue='main_queue')
    def sendTelegram(result, **context):

        if result[0][-1] == 'BACKUPDB - 192.168.1.45':
            minimum_disk = 90
        else:
            minimum_disk = 80

        output = []
        for i in result:
            for j in i:
                # print(j)
                if j in ['/tmpdir', '/'] and i[-1].endswith('192.168.41.7'):
                    output.append(i)
                elif j in ['/backup'] and i[-1].endswith('192.168.1.45'):
                    output.append(i)
                elif j in ['/'] :
                    output.append(i)

        host = output[0][-1]
        # print(host)
        mention =f'<b>[{host}]</b>\nHi <a href="tg://user?id=905830939">Himmawan</a>, <a href="tg://user?id=42168291">Ririz</a>, please check following status :'
        for i in output:
            percentage = int(i[4].replace("%",""))
            if percentage >= minimum_disk:
                mention = mention+f'\nDisk usage on {i[0]} is <b>{i[4]} {i[5]}</b>'
                message = {
                'text': """{}""".format(mention),
                "disable_web_page_preview": True,
                'disable_notification': False,
                'parse_mode': 'HTML'
                }
            
                telegram_hook = TelegramHook("telegram")
                result = telegram_hook.send_message(message)
            else:
                print("Partition disk is still available")

    start = EmptyOperator(task_id='start', dag=dag, queue='main_queue')

    hostname = ['ssh-cboss-galera-01', 'ssh-cboss-galera-02',
                'ssh-cboss-galera-03', 'ssh-cboss-mhg', 'ssh-cboss-snt', 'ssh-cboss-snl', 'ssh-analysis']	# , 'ssh-backupdb'
    SSHOperator = sshServer.expand(conn_id=hostname)
    sendTelegram = sendTelegram.expand(result=SSHOperator)

    start >> SSHOperator >> sendTelegram
    # start >> SSHOperator 
