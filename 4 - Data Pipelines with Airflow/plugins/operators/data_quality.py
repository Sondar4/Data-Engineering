from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 non_empty_tables=[],
                 not_null_fields={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.non_empty_tables = non_empty_tables
        self.not_null_fields = not_null_fields

    def execute(self, context):
        fails = 0

        # Check that all tables contain at least one row
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Starting non empty tables checks")
        for table in self.non_empty_tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f"Data quality check failed. {table} returned no results")
                fails += 1
            num_records = records[0][0]
            if num_records < 1:
                self.log.info(f"Data quality check failed. {table} contained 0 rows")
                fails += 1
            self.log.info(f"Data quality check on table {table} passed with {num_records} records")

        # Check that specified columns don't have null values
        self.log.info("Starting not null fields checks")
        for table in self.not_null_fields.keys():
            for column in self.not_null_fields[table]:
                nulls = redshift.get_records(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
                if len(nulls) < 1 or len(nulls[0]) < 1:
                    self.log.info(f"Data quality check failed. {table} returned no results")
                    fails += 1
                if nulls[0][0] > 0:
                    self.log.info(f"Data quality check failed. {table} contained {nulls[0][0]} null values on column {column}")
                    fails += 1
                self.log.info(f"Data quality check on table {table} passed. Column {column} has 0 NULL records")

        if fails > 0:
            raise ValueError(f"{fails} data quality checks failed")
