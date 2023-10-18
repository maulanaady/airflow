from datetime import datetime, timedelta
import mysql.connector, airflow, os,logging
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.decorators import task


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
    logging.info('try to get connection ...')
    try:
        conn = mysql.connector.connect(user=username, password=password, host=host, database=database)
        logging.info('connected..')
        return conn
    except Exception as e:
        logging.info(str(e))
        return None

def checkTableExists(dbcon, database_name, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = '{}'
        and table_name = '{}'
        """.format(database_name, tablename))
    result = dbcur.fetchall()
#    logging.info(result[0][0])
    if result[0][0] == 1:
        dbcur.close()
        return True
    else:
        return False        
    dbcur.close() 

def get_monitoring_table(conn):
    import pandas as pd
    import numpy as np
    cursor = conn.cursor()
    cursor.execute("""
        select * from psndba.monitoring;
    """)
    results = cursor.fetchall()
    cursor.close()
 
    cols = ['id', 'database_name', 'table_name', 'is_exists_id', 'max_id','column_identifier','values','list_of_column', 'last_sync_at']
    df = pd.DataFrame(results, columns=cols)
    df2 = df.replace(np.nan, None)
    col = ['values', 'last_sync_at']
    df2[col] = df2[col].astype(object).where(df2[col].notnull(), None)
    df2.to_csv('/opt/airflow/data/monitoring.csv', sep=',', header=True, index=False)

def create_metadata(conn):
    import pandas as pd
    import numpy as np
    df = pd.read_csv('/opt/airflow/data/monitoring.csv', sep=',')
    df2 = df.replace(np.nan, None)
    data = list(df2.itertuples(index=False, name=None))

    cur = conn.cursor()
    cur.execute("""
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

    query = """insert into psndba.monitoring (id, database_name, table_name, is_exists_id, max_id, column_identifier, `values`, list_of_column, last_sync_at) 
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    cur.executemany(query, data)
    conn.commit()

    cur.execute("""
        CREATE DEFINER=`cboss`@`%` PROCEDURE `psndba`.`update_table`(db_name varchar(50), tbl_name varchar(100))
        begin
        DECLARE done INT DEFAULT FALSE;	
        declare v_tmp_tbl VARCHAR(50);
        declare v_final_tbl VARCHAR(50);
        declare original_table varchar(100);
        declare v_id varchar(10);
        declare identifier_log varchar(20);

        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            select SUBSTRING_INDEX(list_of_column ,',',1) into v_id
            from psndba.monitoring 
            where database_name = db_name
            and table_name = tbl_name ;
        
            select column_identifier into identifier_log
            from psndba.monitoring 
            where database_name = db_name
            and table_name = tbl_name ;
        
            select concat(tbl_name,'_temp') into v_tmp_tbl;
            select concat(tbl_name,'_testing') into v_final_tbl;  
            select concat(db_name,'.',tbl_name) into original_table;
            SET autocommit=0;
            set @db := 'psndba';
            set @tmp_tbl := v_tmp_tbl;
            set @final_tbl := v_final_tbl;
            set @table_id := v_id;
            set @identifier := identifier_log;
            set @original_tbl := original_table;
            
        --     if identifier_log is not null then
            set @sql_delete := CONCAT('delete from ',@original_tbl,' where ',@table_id,' in (select ',@table_id,' from ',@db,'.',@tmp_tbl,');');
            -- set @sql_delete := CONCAT('delete from ',@db,'.',@final_tbl,' where id in (select id from ',@db,'.',@tmp_tbl,');');
            PREPARE stmt FROM @sql_delete;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        -- 	end if;
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
                max(',@table_id,') as max_id, 
                max(',@identifier,') as `values` from ',@db,'.',@tmp_tbl,');');
            else
                set @sql_update_monitoring := CONCAT('create temporary table temp_updated as 
                (select \'',@original_tbl,'\' as db_tbl, 
                max(',@table_id,') as max_id from ',@db,'.',@tmp_tbl,');');
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
        end
    """)
    conn.commit()
    cur.close()


def read_log(conn, database, table):
    from mysql.connector import errorcode
    logging.info('try to read log table ...')
    if (conn is not None):
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                query = """select database_name, table_name, is_exists_id, max_id,
                column_identifier, `values`, list_of_column
                from psndba.monitoring where database_name = '{}' and table_name = '{}' and is_exists_id = 1;""".format(database, table)

                cursor.execute(query)
                result = cursor.fetchall()
                logging.info('log table read successfully')
                return result
                cursor.close()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.info("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.info("Database does not exist")
            else:
                logging.info(str(err))
            return None 

def call_procedure(cur, procedure_name, args):
    logging.info(f'start to call procedure {procedure_name}({args})' )
    parameter = tuple(args)
    try:
        cur.callproc(procedure_name, parameter)
    except Exception as e:
        logging.info(str(e))            


def read_source(log):
    logging.info(log)
    database = log[0][0]
    list_of_columns = log[0][6].split(',')
    table_id = list_of_columns[0]
    if log[0][1] == 'adm_trx_log_user_access_v2':
        table_src = 'adm_trx_log_user_access'
    elif log[0][1] == 'radacct_log_v2':
        table_src = 'radacct_log'
    else:
        table_src = log[0][1]  

    max_id = log[0][3]
    column_identifier = log[0][4]
    values = log[0][5]

    if max_id is not None and values is not None:
        start_date = datetime.strftime(values, "%Y-%m-%d %H:%M:%S")
        query = f"""select * from {database}.{table_src} where {table_id} > {max_id} or {column_identifier} > STR_TO_DATE('{start_date}','%Y-%m-%d %H:%i:%S') order by {table_id} limit 100000"""
    elif max_id is not None and values is None:
        if column_identifier is not None:
            query = f"""select * from {database}.{table_src} where {table_id} > {max_id} or {column_identifier} > STR_TO_DATE('2000-01-01 00:00:00','%Y-%m-%d %H:%i:%S') order by {table_id} limit 100000"""        
        else:
            query = f"""select * from {database}.{table_src} where {table_id} > {max_id} order by {table_id} limit 100000"""            
    elif max_id is None and values is not None:
        start_date = datetime.strftime(values, "%Y-%m-%d %H:%M:%S")
        query = f"""select * from {database}.{table_src} where {column_identifier} > STR_TO_DATE('{start_date}','%Y-%m-%d %H:%i:%S')"""        
  
    logging.info(query)

    if table_src == 'radacct_log':
        connection = BaseHook.get_connection('analysis')
        conn = get_connection(username=connection.login, password= connection.password, host=connection.host)   
    else:
        connection = BaseHook.get_connection('cboss-psn')
        conn = get_connection(username=connection.login, password= connection.password, host=connection.host)

    cursor = conn.cursor()
    try:
        logging.info(f'starting to read data from source {table_src}')
        cursor.execute(query)
        result = list(cursor.fetchall())

    except Exception as e:
        logging.info(f'error occured for table: {table_src}, with description {str(e)}')
        result = None

    cursor.close()
    conn.close() 
    return result

def insert_target(log, data, conn_target):
    database = log[0][0]
    table = log[0][1]
    list_of_columns = log[0][6].split(',')
    col = ",".join(list_of_columns)                                    
    sql = f'insert into psndba.{table}_temp ('
    val = '%s,'*len(list_of_columns)
    query_insert = sql + col + ') values (' + val[0:len(val)-1] + ')'  

    cur = conn_target.cursor()
    try:
        logging.info(f'start to insert to temporary table {table}')
        cur.execute(f'create table if not exists psndba.{table}_temp (like {database}.{table})')        
        cur.execute(f'truncate table psndba.{table}_temp')
        cur.executemany(query_insert, data)
        conn_target.commit()
        logging.info(f'records have been inserted to temporary table for {table}. Starting to insert to final table...')
        call_procedure(cur = cur, procedure_name='psndba.update_table', args=[database,table])
        logging.info(f'records inserted to final table {table}')
        # get_monitoring_table(cur)

    except Exception as e:
        logging.info(f'error occured for table: {table} with description: {str(e)}')

    cur.close()    



with DAG(
    dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),
    start_date=airflow.utils.dates.days_ago(1),
    on_failure_callback=dag_failure_alert,
#    on_success_callback=dag_success_alert,
    schedule='0 */2 * * *',
    tags=['mirroring'],
    max_active_runs=1,
    catchup=False,
):
    start = EmptyOperator(task_id='start',queue='main_queue')

#    trigger = TriggerDagRunOperator(
#        task_id="trigger_dagrun",
#        trigger_dag_id=os.path.basename(__file__).replace(".pyc", "").replace(".py", ""),  
#        conf={"message": "Continues run"},
#    )
 
    @task(queue='main_queue')
    def _check_metadata():
        connection= BaseHook.get_connection('mariadb_1316')
        conn = get_connection(username=connection.login, password= connection.password, host=connection.host)
        if not checkTableExists(dbcon=conn, database_name='psndba', tablename='monitoring'):
            logging.info('metadata does not exist, trying to create if first')
            create_metadata(conn)
        else:
            logging.info('metadata exists, skipped')
            pass    
        conn.close()    

    @task(queue='main_queue')
    def execute(db_tbl):
        database_name = list(db_tbl)[0]
        table_name = list(db_tbl)[1]
        connection = BaseHook.get_connection('mariadb_1316')
        conn = get_connection(username=connection.login, password=connection.password, host=connection.host)
        logging.info(checkTableExists(dbcon=conn, database_name='psndba', tablename='monitoring'))
        # if not checkTableExists(dbcon=conn, database_name='psndba', tablename='monitoring'):
        #     create_metadata(conn)
        logs = read_log(conn= conn, database=database_name, table= table_name)
        records = read_source(log=logs)
        insert_target(logs,records, conn)
        conn.close()

    @task(queue='main_queue')
    def _get_monitoring_file():
        connection = BaseHook.get_connection('mariadb_1316')
        conn = get_connection(username=connection.login, password=connection.password, host=connection.host)
        get_monitoring_table(conn)
        conn.close()

    check_metadata = _check_metadata()
    executes = execute.expand(db_tbl=list(zip(['bb_customer','bb_customer','bb_useradmin'], ['cpr_trx_saldo','radacct_log_v2','adm_trx_log_user_access_v2'])))
    get_monitoring_file = _get_monitoring_file()
    
    start >> check_metadata >> executes >> get_monitoring_file