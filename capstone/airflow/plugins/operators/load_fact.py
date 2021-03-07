from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#3633FF'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        truncate_query = f'TRUNCATE TABLE {self.table} ;'
        self.log.info(f'Truncating from {self.table} ...')
        redshift.run(truncate_query)
        self.log.info(f'Truncating from {self.table} - SUCCESS')
        
        insert_query = f'INSERT INTO {self.table} {self.query}'
        self.log.info(f'Inserting into {self.table} ...')
        redshift.run(insert_query)     
        self.log.info(f'Inserting into {self.table} - SUCCESS')