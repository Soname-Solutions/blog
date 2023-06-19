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
          description='DEMO: Retrieves values of variable created directly via AirflowConfigurationOptions',
          schedule_interval=None,
          catchup=False,
          max_active_runs=1
          )

# PYTHON operators definition
def print_defined_vars():
    var_names = ['AIRFLOW__OS_VAR__VARIABLE1','AIRFLOW__ENV__VARIABLE2']
    # Please note the name change:
    # in CloudFormation template:
    # - os_var.variable1
    # - env.variable2
    # Accessing from DAG ("AIRFLOW_" prefix & all uppercase & "." is replaced by "__")
    # - AIRFLOW__OS_VAR__VARIABLE1
    # - AIRFLOW__ENV__VARIABLE2

    for item in var_names:
        value = os.environ[item]
        print(f"---> {item} : {value}")

def print_all_env_vars():
    for item in os.environ:
        value = os.environ[item]
        print(f"---> {item} : {value}")

# test only
def assign_defined_vars():
    var_names = ['AIRFLOW__OS_VAR__VARIABLE1','AIRFLOW__ENV__VARIABLE2']
   
    for item in var_names:
        os.environ[item] = "NEW VAL"        

# DAG definition
with dag as dag:
    print_defined_vars_operator = PythonOperator(
        task_id='print_defined_vars',
        python_callable=print_defined_vars
    )

    print_all_env_vars_operator = PythonOperator(
        task_id='print_all_env_vars',
        python_callable=print_all_env_vars
    )

    print_defined_vars_operator >> print_all_env_vars_operator 
