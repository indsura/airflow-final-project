# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import pendulum

from airflow.decorators import dag,task

from custom_operators.facts_calculator import FactsCalculatorOperator
from custom_operators.has_rows import HasRowsOperator
from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
@dag(start_date=pendulum.now())
def full_pipeline():
#
# TODO: Load trips data from S3 to RedShift. Use the s3_key
#       "data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
#       and YOUR s3_bucket
#
#copy_trips_task = S3ToRedshiftOperator(...)
    empty_task = EmptyOperator(task_id='empty_task')

    
#
# TODO: Perform a data quality check on the Trips table
#
#check_trips = HasRowsOperator(...)

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in RedShift. The fact column should
#       be `tripduration` and the groupby_column should be `bikeid`
#
#calculate_facts = FactsCalculatorOperator(...)
#
# TODO: Define task ordering for the DAG tasks you defined
#
full_pipeline_dag = full_pipeline()