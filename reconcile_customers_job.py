import sys, os
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType, StructType, StructField
from pyspark.sql.types import StringType, DateType, LongType, ArrayType
from pyspark.sql.functions import *
import itertools
import boto3
import time

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucketName' ,'workspace', 'jdbc_url', 'username', 'pswd'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#ccn-alerts-104328-{workspace}-sfmc-migration
workspace = args['workspace']
bucketName = args['bucketName']
jdbc_url = args['jdbc_url']
username = args['username']
pswd = args['pswd']

print('args passed are: {}, {}, {}, {}'.format(workspace, bucketName, jdbc_url, username) )

s3Location = 's3://{}'.format(bucketName)
glue_database = '{}_sfmc_migration'.format(workspace)
outputLocation = ''+s3Location+'/athena-results-ccn-data/'
spark.sql("USE {}".format(glue_database))
spark.sql("DROP TABLE IF EXISTS {}.sfmc_customers_found_in_ccn PURGE".format(glue_database))
spark.sql("DROP TABLE IF EXISTS {}.sfmc_customers_not_found_in_ccn PURGE".format(glue_database))
spark.sql("DROP TABLE IF EXISTS {}.sfmc_customers_not_found_in_acm PURGE".format(glue_database))

athena_client = boto3.client('athena')

sfmc_customers_found_in_ccn = """
CREATE TABLE sfmc_customers_found_in_ccn
WITH (
    format = 'Parquet',
    write_compression = 'SNAPPY',
    external_location = '"""+s3Location+"""/final/data-load/')
AS select cd.customer_id, sd.* from sfmc_data sd join ccn_data cd on trim(sd.cif) = trim(cd.lob_id_value);
"""

sfmc_customers_not_found_in_ccn = """
CREATE TABLE sfmc_customers_not_found_in_ccn
WITH (
    format = 'TEXTFILE',
    write_compression = 'GZIP',
    external_location = '"""+s3Location+"""/final/error-files/sfmc_customers_not_found_in_ccn/',
    field_delimiter = ',')
AS select * from sfmc_data sd where sd.cif not in (select cd.lob_id_value from ccn_data cd where cd.lob_id_value IS NOT NULL);
"""

sfmc_customers_not_found_in_acm = """
CREATE TABLE sfmc_customers_not_found_in_acm 
WITH (
    format = 'TEXTFILE',
    write_compression = 'GZIP',
    external_location = '"""+s3Location+"""/final/error-files/sfmc_customers_not_found_in_acm/',
    field_delimiter = ',')
AS select * from sfmc_data sd where sd.cif not in (select ctg.cif from cif_to_guid ctg where ctg.cif IS NOT NULL) or sd.cif not in (select cg.cupid from cupid_to_guid cg where cg.cupid IS NOT NULL);
"""
query_execution_id_list = []

## sfmc_customers_found_in_ccn
print("sfmc_customers_found_in_ccn: ", sfmc_customers_found_in_ccn)
query_execution_id = athena_client.start_query_execution( QueryString=sfmc_customers_found_in_ccn,
                                                          QueryExecutionContext={
                                                              'Database': glue_database
                                                          },
                                                          ResultConfiguration={
                                                              'OutputLocation': outputLocation
                                                          }
                                                          )
print(query_execution_id)
query_execution_id_list.append(query_execution_id['QueryExecutionId'])



### sfmc_customers_not_found_in_ccn
print("sfmc_customers_not_found_in_ccn: ", sfmc_customers_not_found_in_ccn)
query_execution_id = athena_client.start_query_execution( QueryString=sfmc_customers_not_found_in_ccn,
                                                          QueryExecutionContext={
                                                              'Database': glue_database
                                                          },
                                                          ResultConfiguration={
                                                              'OutputLocation': outputLocation
                                                          }
                                                          )
print(query_execution_id)
query_execution_id_list.append(query_execution_id['QueryExecutionId'])



### sfmc_customers_not_found_in_acm
print("sfmc_customers_not_found_in_acm: ", sfmc_customers_not_found_in_acm)
query_execution_id = athena_client.start_query_execution( QueryString=sfmc_customers_not_found_in_acm,
                                                          QueryExecutionContext={
                                                              'Database': glue_database
                                                          },
                                                          ResultConfiguration={
                                                              'OutputLocation': outputLocation
                                                          }
                                                          )
print(query_execution_id)
query_execution_id_list.append(query_execution_id['QueryExecutionId'])


while(len(query_execution_id_list) != 0):
    for id in query_execution_id_list:
        response_query_execution  = athena_client.get_query_execution(QueryExecutionId= id)
        if(response_query_execution['QueryExecution']['Status']['State'] == 'SUCCEEDED'):
            query_execution_id_list.remove(id)
            print('{} execution completed'.format(id))
    time.sleep(10)

sfmc_customers_found_in_ccn_count = spark.sql('select count(*) as sfmc_customers_found_in_ccn_count from {}.sfmc_customers_found_in_ccn'.format(glue_database))
sfmc_customers_found_in_ccn_count.show()

sfmc_customers_not_found_in_ccn_count = spark.sql('select count(*) as sfmc_customers_not_found_in_ccn_count from {}.sfmc_customers_not_found_in_ccn'.format(glue_database))
sfmc_customers_not_found_in_ccn_count.show()

sfmc_customers_not_found_in_acm_count = spark.sql('select count(*) as sfmc_customers_not_found_in_acm_count from {}.sfmc_customers_not_found_in_acm'.format(glue_database))
sfmc_customers_not_found_in_acm_count.show()

print('JOB Completed!!!')
job.commit()