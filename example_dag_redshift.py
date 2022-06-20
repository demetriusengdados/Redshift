from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

'''clusters'''
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftPauseClusterOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftResumeClusterOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor

'''sql'''
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from include.redshift_custom import RedshiftSQLOperator #temporary until we get bug 22391 fixed

'''transfers'''
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


'''
these variables are here as top-level code for this demonstration which is against Airflow Best Practices
'''
cluster_identifier = 'astronomer-success-redshift'
from_redshift_table = 'from_redshift'
s3_bucket = 'airflow-redshift-demo'
schema = 'fct'
table = 'listing'


with DAG(
    dag_id=f"example_dag_redshift",
    schedule_interval="@daily",
    start_date=datetime(2008, 1, 1),
    end_date=datetime(2008, 1, 7),
    max_active_runs=1,
    template_searchpath='/usr/local/airflow/include/example_dag_redshift',
    catchup=True
) as dag:

    start, finish = [DummyOperator(task_id=tid) for tid in ['start', 'finish']]

    '''
    resuming and pausing clusters requires the following permissions on aws
    - redshift:DescribeClusters
    - redshift:PauseCluster
    - redshift:ResumeCluster
    '''
    resume_redshift = RedshiftResumeClusterOperator(
        task_id='resume_redshift',
        cluster_identifier=cluster_identifier
    )

    cluster_sensor = RedshiftClusterSensor(
        task_id='wait_for_cluster',
        cluster_identifier=cluster_identifier,
        target_status='available'
    )

    '''
    ensure your aws_default connection has read/write access to the s3 bucket that you are referencing
    '''
    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        schema=schema,
        table=from_redshift_table,
        s3_bucket=s3_bucket,
        s3_key=f'{schema}/{from_redshift_table}',
        copy_options=[
            "DELIMITER AS ','"
        ],
        method='REPLACE'
    )

    fct_listing = RedshiftSQLOperator(
        task_id='fct_listing',
        sql='/sql/fct_listing.sql',
        params={
            "schema": schema,
            "table": table
        }
    )

    redshift_to_s3 = RedshiftToS3Operator(
        task_id='fct_listing_to_s3',
        s3_bucket=s3_bucket,
        s3_key=f'{schema}/{table}/{{ ds }}_',
        schema=schema,
        table=table,
        table_as_file_name=False,
        unload_options=[
            "DELIMITER AS ','",
            "FORMAT AS CSV",
            "ALLOWOVERWRITE",
            "PARALLEL OFF",
            "HEADER"
        ]
    )

    pause_redshift = RedshiftPauseClusterOperator(
        task_id='pause_redshift',
        cluster_identifier=cluster_identifier
    )

    start >> resume_redshift >> cluster_sensor >> s3_to_redshift >> fct_listing >> redshift_to_s3 >> pause_redshift >> finish

