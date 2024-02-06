
from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    COPY_SQL = """
    COPY {table}
    FROM '{bucket_path}'
    ACCESS_KEY_ID '{aws_key_id}'
    SECRET_ACCESS_KEY '{aws_password}'
    json '{json_schema_path}'
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                json_schema_path = None,
                is_clear_data = False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_schema_path = json_schema_path
        self.aws_credentials_id = aws_credentials_id
        self.is_clear_data = is_clear_data

        
    def execute(self, context):
        self.log.info(f'StageToRedshiftOperator running to load data to Redshift table {self.table}')

        execution_date = context["execution_date"]

        self.log.info(f"StageToRedshiftOperator Execution datetime is {execution_date}")

        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.is_clear_data: 
            self.log.info(f"Clearing data from destination Redshift table {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        # s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        # We get the hourly updated json files from the corresponding hourly folder in S3
        s3_path = "s3://{}/{}/{}".format(self.s3_bucket, rendered_key, execution_date.hour)

        self.log.info(f"StageToRedshiftOperator s3_path is {s3_path}")

        if self.json_schema_path is not None:
            s3_json_schema_path = "s3://{}/{}".format(self.s3_bucket, self.json_schema_path) 
        else:
            s3_json_schema_path = "auto ignorecase"

        formatted_sql = StageToRedshiftOperator.COPY_SQL.format(
            table = self.table,
            bucket_path = s3_path,
            aws_key_id = aws_connection.login,
            aws_password = aws_connection.password,
            json_schema_path = s3_json_schema_path
        )
        redshift.run(formatted_sql)

        








