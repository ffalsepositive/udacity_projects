from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    ui_color = '#FB4C00'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_path = "/home/workspace/airflow/"
        create_sql = open(sql_path + "create_tables.sql", "r").read()
        create_sql = create_sql.replace("\n", "").replace("\t", "").split(";")
        create_sql = [q for q in create_sql if q != ""]
        for query in create_sql:
            redshift.run(query)
        
        self.log.info('Finished creating tables.')





