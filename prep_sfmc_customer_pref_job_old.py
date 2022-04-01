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
data_path = s3Location +'/final/data-load/'
output_path = s3Location + '/ccn-data-load'

#s3Location = 's3://ccn-alerts-104328-dev-sfmc-migration/output/sfmc-data/SFMC-ONETIME-MOB-CONTACT-PREF-20220218010101/'

############# Read File & Transform with Alerts by Channel #################
ccn_headers = """GUID_CIF,CIF,MOBILE_NUMBER,EMAIL,MOBILE_VERIFICATION_STATUS,MOBILE_VERIFICATION_TIMESTAMP,\
password-hint_SMS,password-hint_EMAIL,security-info-change_SMS,security-info-change_EMAIL,new-notice-available_SMS,new-notice-available_EMAIL,deposit-statement-available_SMS,deposit-statement-available_EMAIL,deposits-account-balance_SMS,deposits-account-balance_EMAIL,deposits-secure-message-reminder_SMS,deposits-secure-message-reminder_EMAIL,deposits-secure-message-sent_SMS,deposits-secure-message-sent_EMAIL,deposit-confirmation_SMS,deposit-confirmation_EMAIL,deposits-closed-account_SMS,deposits-closed-account_EMAIL,access-blocked-failed-password-change_SMS,access-blocked-failed-password-change_EMAIL,access-locked-authentication-failure_SMS,access-locked-authentication-failure_EMAIL,overdraft-transfer_SMS,overdraft-transfer_EMAIL,overdrawn-account_SMS,overdrawn-account_EMAIL,distribution-request-received_SMS,distribution-request-received_EMAIL,cd-raise-your-rate_SMS,cd-raise-your-rate_EMAIL,username-change_SMS,username-change_EMAIL,deposits-alert-settings-changed_SMS,deposits-alert-settings-changed_EMAIL,statements-delivery-preference-changed_SMS,statements-delivery-preference-changed_EMAIL,phone-banking-pin-changed_SMS,phone-banking-pin-changed_EMAIL,deposits-email-changed-new-address_SMS,deposits-email-changed-new-address_EMAIL,deposits-address-changed_SMS,deposits-address-changed_EMAIL,otp-challenge-bypassed_SMS,otp-challenge-bypassed_EMAIL,otp-delivery-method-added_SMS,otp-delivery-method-added_EMAIL,debit-transaction-threshold-amount_SMS,debit-transaction-threshold-amount_EMAIL,cd-renewal-instructions-submitted_SMS,cd-renewal-instructions-submitted_EMAIL,cd-maturity-instructions-updated_SMS,\
cd-maturity-instructions-updated_EMAIL,exceeded-transaction-limit_SMS,exceeded-transaction-limit_EMAIL"""


def concatColumnwithNames(df):
    columns = df.columns
    new_col = array()
    for col in columns:
        if col.endswith(('_SMS', '_EMAIL')):
            new_val = concat_ws(":", array(lit(col), col))
            new_col = array_union(new_col, array(new_val))
    return new_col


pref_df_0 = glueContext.create_dynamic_frame.from_options(
    connection_type="parquet",
    connection_options={"paths": [data_path]})

ccn_headers_list = ccn_headers.split(",")
mapping = []

if(len(pref_df_0.schema().fields) != len(ccn_headers_list)):
    print("headers doesnt match!!! ERROR!!!")

for x in range(0, len(pref_df_0.schema().fields)):
    mapping.append((
        pref_df_0.schema().fields[x].name,
        ccn_headers_list[x]
    ))

pref_df_0 = pref_df_0.apply_mapping(mapping)
pref_df_0 = pref_df_0.toDF()
pref_df_1 = pref_df_0.withColumn("ALERT_CHANNEL_ARRAY", concatColumnwithNames(pref_df_0))
pref_df_11 = pref_df_1.select("GUID_CIF", "MOBILE_NUMBER", "EMAIL", "MOBILE_VERIFICATION_STATUS",
                              "MOBILE_VERIFICATION_TIMESTAMP", "ALERT_CHANNEL_ARRAY")
pref_df_2 = pref_df_11.select("*", explode("ALERT_CHANNEL_ARRAY").alias("col_with_val")).drop("ALERT_CHANNEL_ARRAY")
split_col_with_val = split("col_with_val", ":")
pref_df_3 = pref_df_2.withColumn("ALERT_WITH_CHANNEL", split_col_with_val.getItem(0)).withColumn("FLAG",
                                                                                                 split_col_with_val.getItem(
                                                                                                     1)).drop(
    "col_with_val")
pref_df_31 = pref_df_3.withColumn("ALERT_WITH_CHANNEL_ARRAY", split("ALERT_WITH_CHANNEL", "_")).drop(
    "ALERT_WITH_CHANNEL")
pref_df_4 = pref_df_31.withColumn("CHANNEL", element_at("ALERT_WITH_CHANNEL_ARRAY", size("ALERT_WITH_CHANNEL_ARRAY"))) \
    .withColumn("ALERT_WITH_CHANNEL_ARRAY", array_remove("ALERT_WITH_CHANNEL_ARRAY", "EMAIL")) \
    .withColumn("ALERT_WITH_CHANNEL_ARRAY", array_remove("ALERT_WITH_CHANNEL_ARRAY", "SMS"))
pref_df_5 = pref_df_4.withColumn("ALERT", concat_ws("_", "ALERT_WITH_CHANNEL_ARRAY")).drop("ALERT_WITH_CHANNEL_ARRAY")
pref_df = pref_df_5.select("GUID_CIF", "MOBILE_NUMBER", "EMAIL", "MOBILE_VERIFICATION_STATUS",
                           "MOBILE_VERIFICATION_TIMESTAMP", "ALERT", "CHANNEL", "FLAG")



pref_df.printSchema()
#pref_df.show(truncate=False)

## ALERT
alert_db_df = spark.read.jdbc(jdbc_url, "ALERT",
                              properties={"user": username, "password": pswd, "driver": 'com.mysql.jdbc.Driver'})
channels_schema = ArrayType(StructType([
    StructField("name", StringType(), True),
    StructField("required", StringType(), True),
    StructField("defaultDelivery", StringType(), True),
    StructField("supported", StringType(), True)
]))

alert_df = alert_db_df.select('ALERT_CD', 'STATUS', 'LOB_ID', 'DELIVERY_REQUIRED', 'CUST_PREF_MGMT_SUPPORTED',
                              'CHANNELS').alias('alert_df')
alert_df = alert_df.withColumn("CHANNELS_ARRAY", from_json(col("CHANNELS"), channels_schema)).drop("CHANNELS")
alert_df = alert_df.select("*", explode("CHANNELS_ARRAY").alias("CHANNELS_DETAILS")).drop("CHANNELS_ARRAY")
alert_df = alert_df.withColumn("CHANNEL", upper(col("CHANNELS_DETAILS.name"))) \
    .withColumn("REQUIRED", col("CHANNELS_DETAILS.required").cast(BooleanType())) \
    .withColumn("DEFAULTDELIVERY", col("CHANNELS_DETAILS.defaultDelivery").cast(BooleanType())) \
    .withColumn("SUPPORTED", col("CHANNELS_DETAILS.supported").cast(BooleanType())) \
    .drop("CHANNELS_DETAILS")
alert_df.printSchema()
#alert_df.show(truncate=False)


############# DATA MERGE  #################
# CUSTOMER_CONTACT:-
customer_contact_df = pref_df.withColumn("COMMUNICATION_VAL", when(pref_df.CHANNEL == 'SMS', col("MOBILE_NUMBER")).when(
    pref_df.CHANNEL == 'EMAIL', col("EMAIL")))

customer_contact_df = customer_contact_df.select(col("GUID_CIF").alias("CUSTOMER_ID"), col("CHANNEL").alias("COMMUNICATION_TYPE"), "COMMUNICATION_VAL",
                                                 lit("primary").alias("PRIORITY"))
customer_contact_df = customer_contact_df.filter(col("COMMUNICATION_TYPE") == "SMS")
customer_contact_df = customer_contact_df.distinct()
customer_contact_df.printSchema()
#customer_contact_df.show(truncate=False)

customer_mobile_consent_df = customer_contact_df.select(col("COMMUNICATION_VAL").alias("MOBILE_NUMBER"), col("CUSTOMER_ID"),
                                                        lit("deposit").alias("LOB_ID"),
                                                        lit("1").alias("CSR"), lit("1").alias("TMC"),
                                                        lit("1").alias("PRM"))

customer_mobile_consent_df.printSchema()
#customer_mobile_consent_df.show(truncate=False)

## CUSTOMER_ALERT_PREFERENCE:-
# Check if ALERT CHANNEL is valid
alert_df_alert = pref_df.join(alert_df, (pref_df.ALERT == alert_df.ALERT_CD) & (pref_df.CHANNEL == alert_df.CHANNEL), "left") \
    .select(pref_df["*"],alert_df.REQUIRED, alert_df.DEFAULTDELIVERY, alert_df.SUPPORTED)  #alert_df_test.STATUS,alert_df_test.ALERT_CD, alert_df_test.CHANNEL


# filter out if REQUIRED=False & FLAG = F
alert_df_alert_required = alert_df_alert.filter((alert_df_alert.REQUIRED == False) & (alert_df_alert.FLAG == 'T') )

cap_df_1 = alert_df_alert_required.select("GUID_CIF", "ALERT", "CHANNEL", "FLAG")
cap_df_2 = cap_df_1.groupby("GUID_CIF", "ALERT").pivot("CHANNEL").agg(first("FLAG"))

cap_df_1.printSchema()
#cap_df_1.show(truncate=False)
print('#'*20)
cap_df_2.printSchema()
#cap_df_2.show(truncate=False)

def bool_to_binary(col):
    return when(col == 'T', "1") \
        .when(col == 'F', "0") \
        .otherwise(None)

if("EMAIL" in cap_df_2.columns):
    cap_df_2 = cap_df_2.withColumn("EMAIL", bool_to_binary(col("EMAIL")))

cap_df_2 = cap_df_2.withColumn("SMS", bool_to_binary(col("SMS")))
cap_df = cap_df_2.withColumnRenamed("GUID_CIF", "CUSTOMER_ID").withColumnRenamed("ALERT", "ALERT_CD") \
    .withColumnRenamed("SMS", "DELIVER_SMS").withColumnRenamed("EMAIL", "DELIVER_EMAIL")

cap_df.printSchema()
#cap_df.show(truncate=False)


############# WRITE TO GLUE Tables #################
print('Data extraction completed, creating glue tables')
spark.sql("USE {}".format(glue_database))
spark.sql("DROP TABLE IF EXISTS {}.customer_contact PURGE".format(glue_database))
spark.sql("DROP TABLE IF EXISTS {}.customer_mobile_consent PURGE".format(glue_database))
spark.sql("DROP TABLE IF EXISTS {}.customer_alert_preference PURGE".format(glue_database))

customer_contact_df.registerTempTable("customer_contact_df")
customer_mobile_consent_df.registerTempTable("customer_mobile_consent_df")
cap_df.registerTempTable("cap_df")

spark.sql("""
    CREATE TABLE {}.customer_contact
    USING PARQUET
    LOCATION '{}/customer_contact'
    AS SELECT * FROM customer_contact_df
""".format(glue_database, output_path))

spark.sql("""
    CREATE TABLE {}.customer_mobile_consent
    USING PARQUET
    LOCATION '{}/customer_mobile_consent'
    AS SELECT * FROM customer_mobile_consent_df
""".format(glue_database, output_path))

spark.sql("""
    CREATE TABLE {}.customer_alert_preference
    USING PARQUET
    LOCATION '{}/customer_alert_preference'
    AS SELECT * FROM cap_df
""".format(glue_database, output_path))


customer_contact_count = spark.sql('select count(*) as customer_contact_count from {}.customer_contact'.format(glue_database))
customer_contact_count.show()

customer_mobile_consent_count = spark.sql('select count(*) as customer_mobile_consent_count from {}.customer_mobile_consent'.format(glue_database))
customer_mobile_consent_count.show()

customer_alert_preference_count = spark.sql('select count(*) as customer_alert_preference_count from {}.customer_alert_preference'.format(glue_database))
customer_alert_preference_count.show()

print('JOB Completed!!!')
job.commit()
