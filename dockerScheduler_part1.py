from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello world!'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['xunpeng715@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
        'docker_part1', default_args=default_args, schedule_interval=timedelta(minutes=10))

t1 = PythonOperator(task_id='task_1', python_callable=print_hello, dag=dag)

t2 = BashOperator(
    task_id='task_2',
    bash_command='docker run -ti xunpeng715/ads_homework1_part1:2',
    dag=dag)

t2.set_upstream(t1)
