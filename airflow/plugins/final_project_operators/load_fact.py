from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Custom Airflow Operator to load data into a fact table in Amazon Redshift.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Airflow connection ID for Redshift
                 table="",             # Target fact table
                 sql_query="",         # SQL query that produces rows to be inserted
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        # Save parameters to instance
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):

        # Log the start of loading
        self.log.info(f'Loading data into fact table: {self.table}')

        # Connect to Redshift using the specified connection ID
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Format full INSERT INTO statement
        formatted_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query}
        """
        # Log and run the SQL
        self.log.info(f"Executing SQL: {formatted_sql}")
        redshift.run(formatted_sql)

        self.log.info(f"Successfully loaded data into fact table: {self.table}")