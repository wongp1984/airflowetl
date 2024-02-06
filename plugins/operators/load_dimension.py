from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql_insert = "",
                 is_clear_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.is_clear_data = is_clear_data

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator running to load the dimension table {self.table}.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.is_clear_data :
            self.log.info(f"Clearing data from Redshift table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Append data into Redshift table {self.table}")
        redshift.run(self.sql_insert)