import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.functions import *
import boto3
import logging
from botocore.exceptions import ClientError
import pymysql
import re

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

# workspace can be any of dev, qa-r-qa2, psp, prod
glue_database = '{}_sfmc_migration'.format(workspace)
############################## CUSTOMER_CONTACT ##############################


customer_contact_df = glueContext.create_dynamic_frame_from_catalog(glue_database, "customer_contact")
customer_contact_df.printSchema()
customer_contact_df = customer_contact_df.toDF()
#customer_contact_df.show(truncate=False)

#glueContext.write_dynamic_frame.from_jdbc_conf(frame = customer_contact_df, catalog_connection = "mysql-db",
#                                               connection_options = {"dbtable": "CUSTOMER_CONTACT", "database": "ALERT"})

customer_contact_db_df = spark.read.jdbc(jdbc_url, "CUSTOMER_CONTACT", properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})


customer_contact_common_df = customer_contact_df.join(customer_contact_db_df, (customer_contact_df.CUSTOMER_ID == customer_contact_db_df.CUSTOMER_ID)
                                                      & (customer_contact_df.COMMUNICATION_TYPE == customer_contact_db_df.COMMUNICATION_TYPE) ,"left").select(customer_contact_df["*"], customer_contact_db_df.CUSTOMER_CONTACT_ID, customer_contact_db_df.CREATED_BY)
print('####################customer_contact_common_df##############################')
#customer_contact_common_df.show(truncate=False)


customer_contact_insert_df = customer_contact_common_df.filter((col("CREATED_BY").isNull()) | (col("CREATED_BY") == "SFMC_MIGRATION")) \
    .drop("CUSTOMER_CONTACT_ID").drop("CREATED_BY") \
    .withColumn("CREATED_DT", current_timestamp()) \
    .withColumn("CREATED_BY", lit("SFMC_MIGRATION")) \
    .withColumn("UPDATED_BY", lit(None).cast(StringType()))

#customer_contact_insert_df = customer_contact_common_df.withColumn("CREATED_DT", when(customer_contact_common_df.CUSTOMER_CONTACT_ID.isNull(), current_timestamp()).otherwise(lit(None).cast('string'))) \
#    .withColumn("CREATED_BY", when(customer_contact_common_df.CUSTOMER_CONTACT_ID.isNull(), lit("SFMC_MIGRATION")).otherwise(lit(None).cast('string'))) \
#    .withColumn("UPDATED_DT", when(customer_contact_common_df.CUSTOMER_CONTACT_ID.isNotNull(), current_timestamp()).otherwise(lit(None).cast('string'))) \
#    .withColumn("UPDATED_BY", when(customer_contact_common_df.CUSTOMER_CONTACT_ID.isNotNull(), lit("SFMC_MIGRATION")).otherwise(lit(None).cast('string')))
#customer_contact_insert_df = customer_contact_common_df.drop("CUSTOMER_CONTACT_ID")

records_to_delete = customer_contact_common_df.filter(col("CREATED_BY") == "SFMC_MIGRATION").select("CUSTOMER_CONTACT_ID")

print('####################records_to_delete##############################')
#records_to_delete.show(truncate=False)

print('####################customer_contact_insert_df##############################')
#customer_contact_insert_df.show(truncate=False)


def delete_records(partitionData):
    id_list = []
    for row in partitionData:
        id_list.append(row['CUSTOMER_CONTACT_ID'])
    print('updating by delete ids: {}'.format(id_list))
    if(len(id_list) != 0):
        id_string = ",".join(map(str, id_list))
        delete_sql_cmc = 'delete from {} where {} in ({}) AND CREATED_BY = "{}"'.format('CUSTOMER_MOBILE_CONSENT', 'CUSTOMER_CONTACT_ID', id_string, 'SFMC_MIGRATION')
        delete_sql_cc = 'delete from {} where {} in ({}) AND CREATED_BY = "{}"'.format('CUSTOMER_CONTACT', 'CUSTOMER_CONTACT_ID', id_string, 'SFMC_MIGRATION')
        print('delete cmc: {}'.format(delete_sql_cmc))
        print('delete cc: {}'.format(delete_sql_cc))
        host = re.search('://(.*):3306', jdbc_url).group(1)
        conn = pymysql.connect(host= host ,user= username,password = pswd ,db='ALERT')
        cur = conn.cursor()
        cur.execute(delete_sql_cmc)
        cur.execute(delete_sql_cc)
        output = cur.fetchall()
        conn.commit()
        conn.close()
        print(output)

print('delete of existing records to update started')

records_to_delete.rdd.coalesce(10).foreachPartition(delete_records)

print('delete of existing records to update complete')

print('write to CUSTOMER_CONTACT started')

customer_contact_insert_df.write.mode("append").jdbc(jdbc_url,"CUSTOMER_CONTACT", properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})

print('write to CUSTOMER_CONTACT completed')
############################### CUSTOMER_MOBILE_CONSENT ##############################
#
customer_mobile_consent_df = glueContext.create_dynamic_frame_from_catalog(glue_database, "customer_mobile_consent")
customer_mobile_consent_df.printSchema()
customer_mobile_consent_df = customer_mobile_consent_df.toDF()
print('####################customer_mobile_consent_df##############################')
#customer_mobile_consent_df.show(truncate=False)

customer_contact_pk_db_df = spark.read.jdbc(jdbc_url, "CUSTOMER_CONTACT", properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})

#customer_contact_pk = customer_contact_pk_db_df.join(customer_contact_insert_df, customer_contact_pk_db_df.CUSTOMER_ID == customer_contact_insert_df.CUSTOMER_ID, "inner").select(customer_contact_pk_db_df.CUSTOMER_CONTACT_ID, customer_contact_pk_db_df.CUSTOMER_ID)

customer_mobile_consent_df = customer_mobile_consent_df.join(customer_contact_pk_db_df, customer_mobile_consent_df.CUSTOMER_ID == customer_contact_pk_db_df.CUSTOMER_ID, "left"). \
    filter(col("COMMUNICATION_TYPE") == 'SMS'). \
    select(customer_mobile_consent_df["*"], customer_contact_pk_db_df.CUSTOMER_CONTACT_ID, customer_contact_pk_db_df.CREATED_BY)

#customer_mobile_consent_df.show(truncate=False)

customer_mobile_consent_df = customer_mobile_consent_df.filter((col("CREATED_BY").isNull()) | (col("CREATED_BY") == "SFMC_MIGRATION")) \
    .drop("CREATED_BY") \
    .withColumn("CREATED_DT", current_timestamp()) \
    .withColumn("CREATED_BY", lit("SFMC_MIGRATION"))

print('####################customer_mobile_consent_df##############################')
#customer_mobile_consent_df.show(truncate=False)

print('write to CUSTOMER_MOBILE_CONSENT started')

customer_mobile_consent_df.write.mode("append").jdbc(jdbc_url,"CUSTOMER_MOBILE_CONSENT", properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})

print('write to CUSTOMER_MOBILE_CONSENT completed')

#
############################### CUSTOMER_ALERT_PREFERENCE ##############################
#
#customer_alert_preference_df = glueContext.create_dynamic_frame_from_catalog(glue_database, "customer_alert_preference")
#customer_alert_preference_df.printSchema()
#customer_alert_preference_df.toDF().show(truncate=False)




print('JOB Completed!!!')
job.commit()
