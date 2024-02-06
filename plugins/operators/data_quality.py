from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_checks={},
                 up_stream_id = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_checks = table_checks
        self.redshift_conn_id = redshift_conn_id
        self.up_stream_id = up_stream_id
        self.task_id = kwargs['task_id']
        

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        for table, compare_row_fct in self.table_checks.items():
            self.log.info(f'DataQualityOperator checking table {table}')

            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            
            if self.up_stream_id is None:
                self.log.info(f'DataQualityOperator push row counts={num_records} for {table} with to Xcom')
                context['task_instance'].xcom_push(key=table, value=num_records)

               
            else:
                prev_cnt = context['task_instance'].xcom_pull(task_ids=self.up_stream_id, key=table) 

                self.log.info(f'DataQualityOperator retrieved row counts={prev_cnt} for {table} from to Xcom which was passed from {self.up_stream_id}')

                # if num_records < prev_cnt:
                if not compare_row_fct(prev_cnt, num_records) :
                    raise ValueError(f"Data quality check failed. {table} contained < {prev_cnt} rows")

                self.log.info(f"Data quality on table {table} check passed with record count {records[0][0]} >= {prev_cnt}")

        
                

           

    
