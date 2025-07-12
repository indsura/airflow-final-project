from airflow.hooks.postgres_hook import PostgresHook 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases or []

    def execute(self, context):
        self.log.info("Starting data quality checks...")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.test_cases:
            raise ValueError("No data quality test cases provided.")

        for index, test in enumerate(self.test_cases):
            sql = test.get("sql")
            expected = test.get("expected_result")

            if not sql or expected is None:
                raise ValueError(f"Test case {index + 1} is missing 'sql' or 'expected_result'.")

            self.log.info(f"Running data quality check {index + 1}: {sql}")
            records = redshift.get_records(sql)

            if not records or records[0][0] != expected:
                raise ValueError(f"Data quality check {index + 1} failed. SQL: {sql}, "
                                 f"Expected: {expected}, Got: {records[0][0] if records else 'No result'}")

            self.log.info(f"Data quality check {index + 1} passed.")

        self.log.info("All data quality checks passed successfully.")
