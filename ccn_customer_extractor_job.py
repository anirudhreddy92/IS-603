import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import logging
from botocore.exceptions import ClientError
from awsglue.dynamicframe import DynamicFrame


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

query_string = "(SELECT CUSTOMER_ID, LOB_ID, LOB_ID_TYPE, LOB_ID_VALUE from CUSTOMER_LOB where LOB_ID in ('deposit', 'idm', 'invest')) as CUSTOMER_LOB"

customer_db_df = spark.read.jdbc(url=jdbc_url, table=query_string,
                                 properties={"user": username, "password": pswd, "driver": 'com.mysql.cj.jdbc.Driver'})
customer_db_df.printSchema()
print("CUSTOMER_LOB count: {}".format(customer_db_df.count()))
#customer_db_df.show(truncate=False)
print('Customer extraction completed, writing to S3!!')
output_folder = s3Location+'/output/ccn-data'
customer_db_df.write.parquet(output_folder, mode="overwrite")
print('JOB Completed!!!')
job.commit()

SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."ccn_data";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."cif_to_guid";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."cupid_to_guid";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."sfmc_data";

SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."sfmc_customers_found_in_ccn";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."sfmc_customers_not_found_in_acm";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."sfmc_customers_not_found_in_ccn";

SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."customer_contact";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."customer_mobile_consent";
SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."customer_alert_preference";


ccn_data,cif_to_guid,cupid_to_guid,sfmc_data
sfmc_customers_found_in_ccn, sfmc_customers_not_found_in_acm, sfmc_customers_not_found_in_ccn
customer_contact, customer_mobile_consent, customer_alert_preference

SELECT count(*) FROM "ccn_alerts_104328_psp_sfmc_migration"."customer_contact" \



