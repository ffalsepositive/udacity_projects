from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#FBF000'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            c1 = len(records)
            c2 = len(records[0])
            c3 = records[0][0]
                                                
            if  (c1 < 1) | (c2 < 1) | (c3 < 1):
                self.log.error("Data quality check failed. {} returned no results".format(table))
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            else:
                self.log.info("Data quality on table {} check passed with {} records".format(table, c3))