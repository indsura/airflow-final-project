from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # Allows `s3_key` to be templated using execution context (e.g., {{ ds }})

    template_fields = ("s3_key",)

    # Base COPY SQL command (parameterized)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 table='', # Target Redshift table
                 redshift_conn_id='', # Airflow connection ID for Redshift
                 aws_credentials_id='',  # Airflow connection ID for AWS credentials
                 region='',  # AWS region (e.g., us-east-1)
                 s3_bucket='',  # S3 bucket name (without s3:// prefix)
                 s3_key='',  # S3 key or key template (templated field)
                 json_option='auto',  # JSON path or 'auto'
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Assign parameters to instance
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        
    def execute(self, context):

        # Retrieve AWS credentials from Airflow metastore connection

        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)

        # Connect to Redshift via PostgresHook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #Clear target table before loading (common for staging tables)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        # Use templated context to render s3_key dynamically (e.g., based on execution date)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Log and run COPY command
        self.log.info(f"Copying data from S3 path: {s3_path} to Redshift table: {self.table}")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.region, 
            self.json_option
        )
        self.log.info("Running COPY command...")
        redshift.run(formatted_sql)
        self.log.info(f"Successfully loaded data into {self.table}")