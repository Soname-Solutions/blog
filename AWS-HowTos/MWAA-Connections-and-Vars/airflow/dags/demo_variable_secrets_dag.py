from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os

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
          description='DEMO: Usage of variables stored in AWS Secrets Manager',
          schedule_interval=None,
          catchup=False,
          max_active_runs=1
          )

# operators definition
def print_vars_from_secret():
    var_names = ['test_variable'] # searches for secret named "${ProjectName}/variables/test_variable"
    for item in var_names:
        value = Variable.get(item)
        print(f"---> {item} : {value}")


# DAG definition
with dag as dag:
    print_vars_from_secret_operator = PythonOperator(
        task_id='print_vars_from_secret',
        python_callable=print_vars_from_secret
    )    
