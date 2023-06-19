import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Connection

from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

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

def execute_pgsql_json():
    conn_id = "aurora_postgresql_json" # refers to secret named <connection_prefix>/aurora_postgresql_json
    sql = SELECT_SQL

    result = []
    mysql_hook = PostgresHook(postgres_conn_id=conn_id)
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    if cursor.description:
        result = cursor.fetchall()
        print(result)
    return result

def get_connection():
    conn_ids = ["aurora_postgresql_uri", "aurora_postgresql_json" ] 

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

    pgsql_operator = PostgresOperator(
        task_id='pgsql',
        postgres_conn_id = 'aurora_postgresql_uri', # refers to secret named <connection_prefix>/aurora_postgresql_uri
        sql=CREATE_SQL
    )

    execute_pgsql_json_operator = PythonOperator(
        task_id='execute_pgsql_json',
        python_callable=execute_pgsql_json
    )

    get_connection_operator = PythonOperator(
        task_id='get_connection',
        python_callable=get_connection,
        trigger_rule='all_done'
    )

    dummy_operator  >> [pgsql_operator, execute_pgsql_json_operator, get_connection_operator]
    