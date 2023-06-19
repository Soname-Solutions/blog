import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Connection

from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook

CREATE_SQL = "CREATE TABLE IF NOT EXISTS t1 (id int);"
SELECT_SQL = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.tables LIMIT 10"

DAG_ID = os.path.basename(__file__).replace('_dag.py', '')
default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(f'{DAG_ID}',
          default_args=default_args,
          description='DEMO: Usage of credentials stored in AWS Secrets Manager',
          schedule_interval=None,
          catchup=False,
          max_active_runs=1
          )

def execute_sql_json():
    conn_id = "aurora_mysql_json" # refers to secret named <connection_prefix>/aurora_mysql_json
    sql = SELECT_SQL

    result = []
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    if cursor.description:
        result = cursor.fetchall()
        print(result)
    return result

def get_connection():
    conn_ids = ["aurora_mysql_uri", "aurora_mysql_json" ]

    for conn_id in conn_ids:
        print(f"*** Processing connection: {conn_id}")

        connection = Connection.get_connection_from_secrets(conn_id)
        print(connection)
        print(f"URI: {connection.get_uri()}")
        print(f"Connection Properties: {connection.host}, {connection.login}, {connection.password}, {connection.extra}")


# DAG definition
with dag as dag:
    dummy_operator = DummyOperator(
        task_id='start'
    )

    mysql_uri_operator = MySqlOperator(
        task_id='mysql_uri_connection',
        mysql_conn_id = 'aurora_mysql_uri', # refers to secret named <connection_prefix>/aurora_mysql_uri
        sql=CREATE_SQL
    )

    execute_sql_json_operator = PythonOperator(
        task_id='execute_sql_json_connection',
        python_callable=execute_sql_json
    )

    get_connection_operator = PythonOperator(
        task_id='get_connection',
        python_callable=get_connection,
        trigger_rule='all_done'
    )

    dummy_operator  >> [mysql_uri_operator, execute_sql_json_operator, get_connection_operator]
    