# Customer Success Redshift Tutorial

This DAG demonstrates how to use the following:

- `RedshiftSQLOperator`
- `RedshiftPauseClusterOperator`
- `RedshiftResumeClusterOperator`
- `RedshiftClusterSensor`
- `RedshiftToS3Operator`
- `S3ToRedshiftOperator`

# Prerequisites

- [Astro CLI](https://docs.astronomer.io/software/quickstart) or [Astrocloud CLI](https://docs.astronomer.io/astro/install-cli)
- Accessible Redshift Cluster
- Account with read/write access to an S3 Bucket
- Airflow Instance (If you plan on deploying)

# Steps to Use

>If you are using the `astro` CLI instead of the `astrocloud` CLI, you can simply replace `astrocloud` in the below commands with `astro`

### Run the following in your terminal:

1. `git clone git@github.com:astronomer/cs-tutorial-databricks.git`
2. `cd cs-tutorial-redshift`
3. `astrocloud dev start`

### Add `redshift_default` & `aws_default` connections to your sandbox

1. Go to your sandbox [http://locahost:8080/home](http://locahost:8080/home)
2. Navigate to connictions (i.e. Admin >> Connections)
3. Add a new connection with the following parameters:

    - Connection Id: redshift_default
    - Connection Type: Amazon Redshift
    - Host: `<Your-Redshift-Endpoint>`
    - Schema: `<Your-Redshift-Database>`
    - Login: `<Your-Redshift-Login>`
    - Password: `<Your-Redshift-Password>`
    - Port: `<Your-Redshift-Port>`


4. Add another connection with the following parameters:

    - Connection Id: aws_default
    - Connection Type: Amazon Web Services
    - Extra: {"aws_access_key_id": "<your-key-id>", "aws_secret_access_key": "<your-secret-access-key>", "region_name": "<your-region>"}
    
> In order to use all of the components from this POC, the account associated with your `aws_default` connection will 
> need the following permissions:
> 
> - Access to perform read/write actions for a pre-configured S3 Bucket
> - Access to interact with the Redshift Cluster, specifically:
>   - `redshift:DescribeClusters`
>   - `redshift:PauseCluster`
>   - `redshift:ResumeCluster`

### Replace variables with values from S3 and Redshift Cluster

In the `redshift_example_dag.py` you'll need to replace variables like `cluster_identifier`, `from_redshift_table`, 
`s3_bucket`, `schema`, and `table` with the corresponding values that actually exist in your Redshift Cluster/S3 Storage

___

After following these steps, you should be able to run the tasks in the `redshift_example_dag`. Enjoy!
