from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",  # SQL insert query or reference to SQL helper
                 truncate_table=True,  # Allows truncate-insert
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f'Connecting to Redshift: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating data from dimension table: {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query}
        """
        self.log.info(f"Executing SQL to load dimension table {self.table}")
        redshift.run(insert_sql)
        self.log.info(f"Successfully loaded data into dimension table: {self.table}")
