from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 create_table=False,
                 create_sql="",
                 delete_previous=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.create_table = create_table
        self.create_sql = create_sql
        self.delete_previous = delete_previous


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_previous:
            self.log.info(f"Deleting dimension table {self.table}")
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")

        if self.create_table:
            self.log.info(f"Creating Redshift fact table {self.table}")
            redshift.run(self.create_sql)

        self.log.info(f"Inserting data into Redshift fact table {self.table}")
        redshift.run(self.sql)