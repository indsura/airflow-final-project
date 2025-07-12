from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Custom Airflow Operator to load data into a dimension table in Amazon Redshift.
    Supports truncate-insert or append-only mode based on parameter.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",   # Airflow connection ID for Redshift
                 table="",              # Target dimension table
                 sql_query="",          # SQL query used to insert data (can be imported from SQL helper)
                 truncate_table=True,   # Whether to truncate the table before insert
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        # Assign arguments to instance variables
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):

        # Connect to Redshift
        self.log.info(f'Connecting to Redshift: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        ## The DAG allows to switch between append-only and delete-load functionality
        if self.truncate_table:
            self.log.info(f"Truncating data from dimension table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        # Compose and execute the insert statement
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query}
        """
        self.log.info(f"Executing SQL to load dimension table {self.table}")
        redshift.run(insert_sql)
        self.log.info(f"Successfully loaded data into dimension table: {self.table}")
