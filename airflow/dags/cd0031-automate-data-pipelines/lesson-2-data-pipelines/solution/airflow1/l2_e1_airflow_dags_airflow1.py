import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World!")


#dag = DAG(
#        'greet_flow_dag_legacy',
#        start_date=pendulum.now())
@dag(
    start_date=pendulum.now()
)
        
@task()
def greet_task(
    hello_world()
)
#greet_task = PythonOperator(
#    task_id="hello_world_task",
#    python_callable=hello_world,
#    dag=dag
#)
