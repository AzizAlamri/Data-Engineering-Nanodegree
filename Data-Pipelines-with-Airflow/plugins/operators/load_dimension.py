from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_insert_query="",
                 table="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert_query = sql_insert_query
        self.append_data=append_data

    def execute(self, context):
        self.log.info(f'Loading the {self.table} table has started')
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_data:
            self.log.info(f'Truncating table {self.table}')
            redshift.run(f'APPEND_DATA {self.table}')
               
        redshift.run(self.sql_insert_query)
        self.log.info(f'Success: Loading the {self.table} table')