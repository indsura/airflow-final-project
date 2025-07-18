from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION {}
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table='',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 region ='',
                 s3_bucket='',
                 s3_key='',
                 json_option ='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        #aws_hook = AwsHook(self.aws_credentials_id)
        #credentials = aws_hook.get_credentials()
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        #rendered_key = self.s3_key.format(**context)
        #s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_key,
            aws_connection.login,
            aws_connection.password,
            self.region, 
            self.json_option
        )
        redshift.run(formatted_sql)