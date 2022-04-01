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

customer_alert_preference_df = glueContext.create_dynamic_frame_from_catalog(glue_database, "customer_alert_preference")
customer_alert_preference_df.printSchema()
customer_alert_preference_df = customer_alert_preference_df.toDF()
#customer_alert_preference_df.show(truncate=False)


customer_alert_preference_db_df = spark.read.jdbc(jdbc_url, "CUSTOMER_ALERT_PREFERENCE", properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})

customer_alert_preference_common_df = customer_alert_preference_df.join(customer_alert_preference_db_df, (customer_alert_preference_df.CUSTOMER_ID == customer_alert_preference_db_df.CUSTOMER_ID) &
                                                                        (customer_alert_preference_df.ALERT_CD == customer_alert_preference_db_df.ALERT_CD),"left") \
    .select(customer_alert_preference_df["*"], customer_alert_preference_db_df.CUSTOMER_ALERT_PREF_ID, customer_alert_preference_db_df.CREATED_BY)

customer_alert_preference_common_df.printSchema()
#customer_alert_preference_common_df.show(truncate=False)
records_to_delete = customer_alert_preference_common_df.filter((col("CREATED_BY") == "SFMC_MIGRATION")).select("CUSTOMER_ALERT_PREF_ID")

customer_alert_preference_common_df = customer_alert_preference_common_df.filter((col("CREATED_BY").isNull()) | (col("CREATED_BY") == "SFMC_MIGRATION")) \
    .drop("CUSTOMER_ALERT_PREF_ID").drop("CREATED_BY") \
    .withColumn("CREATED_DT", current_timestamp()) \
    .withColumn("CREATED_BY", lit("SFMC_MIGRATION"))

def delete_records(partitionData):
    id_list = []
    for row in partitionData:
        id_list.append(row['CUSTOMER_ALERT_PREF_ID'])
    print('updating by delete ids: {}'.format(id_list))
    if(len(id_list) != 0):
        id_string = ",".join(map(str, id_list))
        delete_sql_cap = 'delete from {} where {} in ({}) AND CREATED_BY = "{}" '.format('CUSTOMER_ALERT_PREFERENCE', 'CUSTOMER_ALERT_PREF_ID', id_string, 'SFMC_MIGRATION')
        host = re.search('://(.*):3306', jdbc_url).group(1)
        print('delete cap: {}'.format(delete_sql_cap))
        conn = pymysql.connect(host= host ,user= username,password = pswd ,db='ALERT')
        cur = conn.cursor()
        cur.execute(delete_sql_cap)
        output = cur.fetchall()
        conn.commit()
        conn.close()
        print(output)

print('delete of existing records to update started')

records_to_delete.rdd.coalesce(10).foreachPartition(delete_records)

print('delete of existing records to update completed')

print('write to CUSTOMER_ALERT_PREFERENCE started')

customer_alert_preference_common_df.write.mode("append").jdbc(jdbc_url,"CUSTOMER_ALERT_PREFERENCE", properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})
print('write to CUSTOMER_ALERT_PREFERENCE completed')

#customer_alert_preference_common_df.printSchema()
#customer_alert_preference_common_df.show(truncate=False)

print('JOB Completed!!!')
job.commit()
