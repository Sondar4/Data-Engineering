from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields =("s3_key",)
    copy_sql = """
        COPY {} FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        REGION '{}' COMPUPDATE {}
        JSON '{}' {}
    """
    
    @apply_defaults
    def __init__(self,
                 s3_key: str,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 region="",
                 compupdate="on",
                 json="auto",
                 truncatecolumns="",
                 create_sql="",
                 iam_role="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.compupdate = compupdate
        self.json = json
        self.truncatecolumns = truncatecolumns
        self.create_sql = create_sql
        self.iam_role = iam_role


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing previous data from Redshift staging table {self.table}")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))

        self.log.info(f"Creating Redshift staging table {self.table}")
        redshift.run(self.create_sql)

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.iam_role,
            self.region,
            self.compupdate,
            self.json,
            self.truncatecolumns
        )
        redshift.run(formatted_sql)
