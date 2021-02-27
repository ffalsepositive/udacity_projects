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
        insert_query = f'INSERT INTO {self.table} {self.query}'
        
        delete_query = f'DELETE FROM {self.table}'
        
        self.log.info(f'Deleting from {self.table} ...')
        redshift.run(delete_query)
        self.log.info(f'Deleting from {self.table} - SUCCESS')
        
        self.log.info(f'Inserting into {self.table} ...')
        redshift.run(insert_query)
        self.log.info(f'Inserting into {self.table} - SUCCESS')
