from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#00FB94'
    
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 data_format="JSON",
                 json_path = "auto",
                 delimiter = ",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format
        self.json_path = json_path
        self.delimiter = delimiter

        self.query = """ COPY {} 
                         FROM '{}' 
                         ACCESS_KEY_ID '{}' 
                         SECRET_ACCESS_KEY '{}' """
        
        
    def execute(self, context):
        aws = AwsHook(self.aws_credentials, self.region)
        credentials = aws.get_credentials()
        
        if self.data_format == "JSON":
            self.query = self.query + " REGION '{}' TIMEFORMAT AS 'epochmillisecs' FORMAT AS JSON '{}' ;".format(self.region, self.json_path)
        elif self.data_format == "CSV":
            self.query = self.query + " REGION '{}' TIMEFORMAT AS 'epochmillisecs' FORMAT AS CSV DELIMITER '{}' IGNOREHEADER 1 ;".format(self.delimiter)
        elif self.data_format == "PARQUET":
            self.query = self.query + " FORMAT AS PARQUET ;"
        else:
            self.log.error("Not a valid type of data format.")
        
       
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_query = self.query.format(self.table, 
                                       s3_path,
                                       credentials.access_key,
                                       credentials.secret_key,
                                       self.region)
        
        truncate_query = f'TRUNCATE TABLE {self.table} ;'
        self.log.info(f'Truncating from {self.table} ...')
        redshift.run(truncate_query)
        self.log.info(f'Truncating from {self.table} - SUCCESS')
        
        self.log.info(f'Copying from S3 to {self.table} ...')
        redshift.run(copy_query)
        self.log.info(f'Copying from S3 to {self.table} - SUCCESS')