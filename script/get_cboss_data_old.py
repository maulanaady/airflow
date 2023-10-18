from datetime import date, datetime
import time, asyncio,aiomysql,mysql.connector
from mysql.connector import errorcode
import sys,json,os,requests 
import pandas as pd
import numpy as np
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
import airflow
from airflow import DAG
from airflow.decorators import dag, task


def dag_success_alert(context):
    message = {
        'text': """<b>STATUS</b>: <b>DAG/TASK IS SUCCEED</b>
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


def get_connection(username, password, host, database=None):
    print('try to get connection ...')
    try:
        conn = mysql.connector.connect(user=username, password=password, host=host, database=database)
        print('connected..')
        return conn
    except Exception as e:
        print(str(e))
        return None

def checkTableExists(dbcon, database_name, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = '{}'
        and table_name = '{}'
        """.format(database_name, tablename))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    else:
        return False        
    dbcur.close()        


async def get_monitoring_table(cursor):

    await cursor.execute("""
        select * from psndba.monitoring;
    """)
    results = await cursor.fetchall()
    cols = ['id', 'database_name', 'table_name', 'is_exists_id', 'max_id','column_identifier','values','list_of_column', 'last_sync_at']
    df = pd.DataFrame(results, columns=cols)
    df2 = df.replace(np.nan, None)
    col = ['values', 'last_sync_at']
    df2[col] = df2[col].astype(object).where(df2[col].notnull(), None)
    df2.to_csv('/opt/airflow/data/monitoring.csv', sep=',', header=True, index=False)


async def create_metadata(loop):
    df = pd.read_csv('/opt/airflow/data/monitoring.csv', sep=',')
    df2 = df.replace(np.nan, None)
    data = list(df2.itertuples(index=False, name=None))

    conn = await aiomysql.connect(host='192.168.13.16', port=3306,
                                  user='cboss', password='cboss2018',
                                  db='psndba', loop=loop)

    async with conn.cursor() as cur:
        await cur.execute("""
            CREATE TABLE `psndba`.`monitoring` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `database_name` varchar(50) DEFAULT NULL,
            `table_name` varchar(100) DEFAULT NULL,
            `is_exists_id` tinyint(1) DEFAULT 0,
            `max_id` bigint(20) DEFAULT NULL,
            `column_identifier` varchar(20) DEFAULT NULL,
            `values` timestamp NULL DEFAULT NULL,
            `list_of_column` text DEFAULT NULL,
            `last_sync_at` timestamp NULL DEFAULT NULL,
            PRIMARY KEY (`id`)
            );
        """)
        query = """insert into psndba.monitoring (id, database_name, table_name, is_exists_id, max_id, column_identifier, values, list_of_column, last_sync_at) 
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        await cursor.executemany(query, data)

        await cur.execute("""
            DELIMITER $$
            CREATE DEFINER=`cboss`@`%` PROCEDURE `psndba`.`update_table`(db_name varchar(50), tbl_name varchar(100))
            begin
            DECLARE done INT DEFAULT FALSE;	
            declare v_tmp_tbl VARCHAR(50);
            declare v_final_tbl VARCHAR(50);
            declare original_table varchar(100);
            declare identifier_log varchar(20);

            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

                select column_identifier from psndba.monitoring 
                where database_name = db_name
                and table_name = tbl_name into identifier_log;
            
                select concat(tbl_name,'_temp') into v_tmp_tbl;
                select concat(tbl_name,'_testing') into v_final_tbl;  
                select concat(db_name,'.',tbl_name) into original_table;
                SET autocommit=0;
                set @db := 'psndba';
                set @tmp_tbl := v_tmp_tbl;
                set @final_tbl := v_final_tbl;
                set @identifier := identifier_log;
                set @original_tbl := original_table;
                
                set @sql_delete := CONCAT('delete from ',@original_tbl,' where id in (select id from ',@db,'.',@tmp_tbl,');');
                -- set @sql_delete := CONCAT('delete from ',@db,'.',@final_tbl,' where id in (select id from ',@db,'.',@tmp_tbl,');');
                PREPARE stmt FROM @sql_delete;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
                commit;
                set @sql_insert := CONCAT('insert into ',@original_tbl,' (select * from ',@db,'.',@tmp_tbl,');');
                -- set @sql_insert := CONCAT('insert into ',@db,'.',@final_tbl,' (select * from ',@db,'.',@tmp_tbl,');');
                PREPARE stmt FROM @sql_insert;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
                commit;
            -- 	set @a:= table_name;
                
                drop table if exists temp_updated;
                if identifier_log is not null then
                    set @sql_update_monitoring := CONCAT('create temporary table temp_updated as 
                    (select \'',@original_tbl,'\' as db_tbl, 
                    max(id) as max_id, 
                    max(',@identifier,') as `values` from ',@db,'.',@tmp_tbl,');');
                else
                    set @sql_update_monitoring := CONCAT('create temporary table temp_updated as 
                    (select \'',@original_tbl,'\' as db_tbl, 
                    max(id) as max_id from ',@db,'.',@tmp_tbl,');');
                end if;
                
                PREPARE stmt FROM @sql_update_monitoring;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
                commit;
            --     select * from temp_updated;
                if identifier_log is not null then
                    update psndba.monitoring 
                    INNER join temp_updated 
                    on concat(psndba.monitoring.database_name,'.', psndba.monitoring.table_name) = temp_updated.db_tbl
                    set psndba.monitoring.max_id = coalesce(temp_updated.max_id,psndba.monitoring.max_id),
                    psndba.monitoring.`values` = coalesce(temp_updated.`values`,psndba.monitoring.`values`),
                    psndba.monitoring.last_sync_at = current_timestamp();
                else
                    update psndba.monitoring 
                    INNER join temp_updated 
                    on concat(psndba.monitoring.database_name,'.', psndba.monitoring.table_name) = temp_updated.db_tbl
                    set psndba.monitoring.max_id = coalesce(temp_updated.max_id,psndba.monitoring.max_id),
                    psndba.monitoring.last_sync_at = current_timestamp();	
                end if;
                commit;
            set @sql_truncate := CONCAT('truncate table ',@db,'.',@tmp_tbl,';');      
                PREPARE stmt FROM @sql_truncate;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            commit;
            end;
        """)

    conn.close()



def read_log(conn, database, table):
    print('try to read log table ...')
    if (conn is not None):
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                query = """select database_name, table_name, is_exists_id, max_id,
                column_identifier, `values`, list_of_column
                from psndba.monitoring where database_name = '{}' and table_name = '{}' and is_exists_id = 1;""".format(database, table)

                cursor.execute(query)
                result = cursor.fetchall()
                print('log table read successfully')
                return result
                cursor.close()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(str(err))
            return None    


async def call_procedure(cur, procedure_name, args):
    print(f'start to call procedure {procedure_name}({args})' )
    try:
        await cur.callproc(procedure_name, args)
    except Exception as e:
        print(str(e))            


async def insert_target(list_of_columns, database, table, data, loop):
    col = ",".join(list_of_columns)                                    
    sql = f'insert into psndba.{table}_temp ('
    val = '%s,'*len(list_of_columns)
    query_insert = sql + col + ') values (' + val[0:len(val)-1] + ')'  

    conn_target = await aiomysql.connect(host='192.168.13.16', port=3306,
                                    user='cboss', password='cboss2018',
                                    db='bb_useradmin', autocommit=False, loop=loop)    
    async with conn_target.cursor() as cur:        
        try:
            print(f'start to insert to temporary table {table}')
            await cur.execute(f'truncate table psndba.{table}_temp')
            await cur.execute(f'create table if not exists psndba.{table}_temp (like {database}.{table})')
            # await cur.execute(f'truncate table psndba.{table}_temp')
            await cur.executemany(query_insert, data)
            await conn_target.commit()
            print(f'records have been inserted to temporary table for {table}. Starting to insert to final table...')
            await call_procedure(cur = cur, procedure_name='psndba.update_table', args=[database,table])
            print(f'records inserted to final table {table}')
            await get_monitoring_table(cursor=cur)

        except Exception as e:
            print(f'error occured for table: {table} with description: {str(e)}')

    conn_target.close()


async def read_source(log, loop):
    database = log[0]
    if log[1] == 'adm_trx_log_user_access_v2':
        table_src = 'adm_trx_log_user_access'
    else:
        table_src = log[1]    
    is_exists_id = log[2]
    max_id = log[3]
    column_identifier = log[4]
    values = log[5]
    list_of_columns = log[6].split(',')


    if max_id is not None and values is not None:
        start_date = datetime.strftime(values, "%Y-%m-%d %H:%M:%S")
        query = f"""select * from {database}.{table_src} where id > {max_id} or {column_identifier} > STR_TO_DATE('{start_date}','%Y-%m-%d %H:%i:%S')"""
    elif max_id is not None and values is None:
        if column_identifier is not None:
            query = f"""select * from {database}.{table_src} where id > {max_id} or {column_identifier} > STR_TO_DATE('2000-01-01 00:00:00','%Y-%m-%d %H:%i:%S')"""        
        else:
            query = f"""select * from {database}.{table_src} where id > {max_id}"""            
    elif max_id is None and values is not None:
        start_date = datetime.strftime(values, "%Y-%m-%d %H:%M:%S")
        query = f"""select * from {database}.{table_src} where {column_identifier} > STR_TO_DATE('{start_date}','%Y-%m-%d %H:%i:%S')"""        
  
    print(query)

    
    conn = await aiomysql.connect(host='192.168.41.196', port=3306,
                                    user='cboss', password='cboss2018',
                                    db='bb_useradmin', autocommit=False, loop=loop)    
    async with conn.cursor() as cursor:        
        try:
            print(f'starting to read data from source {table_src}')
            await cursor.execute(query)
            result = list(await cursor.fetchall())
            if table_src == 'adm_trx_log_user_access':
                table_src = 'adm_trx_log_user_access_v2'
            else:
                table_src = table_src

            await insert_target(list_of_columns=list_of_columns, database=database, table=table_src, data=result, loop=loop)
        except Exception as e:
            print(f'error occured for table: {table_src}, with description {str(e)}')
            return None
    conn.close()    


async def main(logs, loop):
    start = 0
    end = len(logs)
    step = 50
    for i in range(start, end, step):
        x = i
        tasks = []
        for log in logs[x:x+step]:
            tasks.append(read_source(log=log, loop=loop))
        await asyncio.gather(*tasks)

with DAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=airflow.utils.dates.days_ago(1),
    on_failure_callback=dag_failure_alert,
    on_success_callback=dag_success_alert,
    schedule_interval='*/15 * * * *',
    max_active_runs=1,
    catchup=False,
):
    start = EmptyOperator(task_id='start')
   
    @task
    def execute(db_tbl):
        database_name = list(db_tbl)[0]
        table_name = list(db_tbl)[1]

        loop = asyncio.get_event_loop()                    
        conn = get_connection(username='cboss', password= 'cboss2018', host='192.168.13.16')
        if checkTableExists(dbcon=conn, database_name='psndba', tablename='monitoring'):
            logs = read_log(conn= conn, database=database_name, table= table_name)
            conn.close()
            asyncio.run(main(logs=logs, loop=loop))
        else:
            create_metadata(loop=loop)
            logs = read_log(conn= conn, database=database_name, table= table_name)
            conn.close()
            asyncio.run(main(logs=logs, loop=loop))


    executes = execute.expand(db_tbl=list(zip(['bb_customer','bb_useradmin'], ['cpr_trx_saldo','adm_trx_log_user_access_v2'])))


    start >> executes
