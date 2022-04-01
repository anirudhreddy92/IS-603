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

print("workspace: ", workspace, ", s3Location: ", s3Location, ", glue_database: ", glue_database, ", outputLocation: ", outputLocation)

spark.sql('DROP DATABASE IF EXISTS {} CASCADE'.format(glue_database))
spark.sql('create database {}'.format(glue_database))

athena_client = boto3.client('athena')

ccn_data_schema = """
CREATE EXTERNAL TABLE `ccn_data`(
  `customer_id` string, 
  `lob_id` string, 
  `lob_id_type` string, 
  `lob_id_value` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'""" + s3Location + """/output/ccn-data/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='sfmc-migration-multi-file-crawler', 
  'classification'='parquet', 
  'compressionType'='none',
  'typeOfData'='file');
"""

cif_to_guid_schema = """
CREATE EXTERNAL TABLE `cif_to_guid`(
  `guid` string, 
  `cif` string, 
  `last_update_dt` string, 
  `inactivated_dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'""" + s3Location + """/output/cif-to-guid/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='sfmc-migration-multi-file-crawler', 
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');
"""

cupid_to_guid_schema = """
CREATE EXTERNAL TABLE `cupid_to_guid`(
  `guid` string, 
  `cupid` string, 
  `last_update_dt` string, 
  `inactivated_dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'""" + s3Location + """/output/cupid-to-guid/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='sfmc-migration-multi-file-crawler', 
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');
"""

sfmc_data_schema = """
CREATE EXTERNAL TABLE `sfmc_data`(
  `cif` string, 
  `mobile_number` string, 
  `email` string, 
  `mobile_verification_status` string, 
  `mobile_verification_timestamp` string, 
  `alert_code_1_sms` string, 
  `alert_code_1_email` string, 
  `alert_code_2_sms` string, 
  `alert_code_2_email` string, 
  `alert_code_3_sms` string, 
  `alert_code_3_email` string, 
  `alert_code_4_sms` string, 
  `alert_code_4_email` string, 
  `alert_code_5_sms` string, 
  `alert_code_5_email` string, 
  `alert_code_6_sms` string, 
  `alert_code_6_email` string, 
  `alert_code_7_sms` string, 
  `alert_code_7_email` string, 
  `alert_code_8_sms` string, 
  `alert_code_8_email` string, 
  `alert_code_9_sms` string, 
  `alert_code_9_email` string, 
  `alert_code_10_sms` string, 
  `alert_code_10_email` string, 
  `alert_code_11_sms` string, 
  `alert_code_11_email` string, 
  `alert_code_12_sms` string, 
  `alert_code_12_email` string, 
  `alert_code_13_sms` string, 
  `alert_code_13_email` string, 
  `alert_code_14_sms` string, 
  `alert_code_14_email` string, 
  `alert_code_15_sms` string, 
  `alert_code_15_email` string, 
  `alert_code_16_sms` string, 
  `alert_code_16_email` string, 
  `alert_code_17_sms` string, 
  `alert_code_17_email` string, 
  `alert_code_18_sms` string, 
  `alert_code_18_email` string, 
  `alert_code_19_sms` string, 
  `alert_code_19_email` string, 
  `alert_code_20_sms` string, 
  `alert_code_20_email` string, 
  `alert_code_21_sms` string, 
  `alert_code_21_email` string, 
  `alert_code_22_sms` string, 
  `alert_code_22_email` string, 
  `alert_code_23_sms` string, 
  `alert_code_23_email` string, 
  `alert_code_24_sms` string, 
  `alert_code_24_email` string, 
  `alert_code_25_sms` string, 
  `alert_code_25_email` string, 
  `alert_code_26_sms` string, 
  `alert_code_26_email` string, 
  `alert_code_27_sms` string, 
  `alert_code_27_email` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'""" + s3Location + """/output/sfmc-data/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='sfmc-migration-multi-file-crawler', 
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file');
"""
query_execution_id_list = []
print("ccn_query_string: ", ccn_data_schema)
query_execution_id = athena_client.start_query_execution( QueryString=ccn_data_schema,
                                                          QueryExecutionContext={
                                                              'Database': glue_database
                                                          },
                                                          ResultConfiguration={
                                                              'OutputLocation': outputLocation
                                                          }
                                                          )
print(query_execution_id)
query_execution_id_list.append(query_execution_id['QueryExecutionId'])


print("cif_to_guid_schema: ", cif_to_guid_schema)
query_execution_id = athena_client.start_query_execution( QueryString=cif_to_guid_schema,
                                                          QueryExecutionContext={
                                                              'Database': glue_database
                                                          },
                                                          ResultConfiguration={
                                                              'OutputLocation': outputLocation
                                                          }
                                                          )
print(query_execution_id)
query_execution_id_list.append(query_execution_id['QueryExecutionId'])


print("cupid_to_guid_schema: ", cupid_to_guid_schema)
query_execution_id = athena_client.start_query_execution( QueryString=cupid_to_guid_schema,
                                                          QueryExecutionContext={
                                                              'Database': glue_database
                                                          },
                                                          ResultConfiguration={
                                                              'OutputLocation': outputLocation
                                                          }
                                                          )
print(query_execution_id)
query_execution_id_list.append(query_execution_id['QueryExecutionId'])


print("sfmc_data_schema: ", sfmc_data_schema)
query_execution_id = athena_client.start_query_execution( QueryString=sfmc_data_schema,
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


ccn_data_count = spark.sql('select count(*) as ccn_data_count from {}.ccn_data'.format(glue_database))
ccn_data_count.show()

cif_to_guid_count = spark.sql('select count(*) as cif_to_guid_count from {}.cif_to_guid'.format(glue_database))
cif_to_guid_count.show()

cupid_to_guid_count = spark.sql('select count(*) as cupid_to_guid_count from {}.cupid_to_guid'.format(glue_database))
cupid_to_guid_count.show()

sfmc_data_count = spark.sql('select count(*) as sfmc_data_count from {}.sfmc_data'.format(glue_database))
sfmc_data_count.show()

print('JOB Completed!!!')
job.commit()



