import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_filepath = 's3://ccn-alerts-104328-psp-ade-relay/pinpoint-data-segment-aos/'

df = spark.read.options(header = 'True', delimiter = ',' , inferSchema = 'False') \
    .format('csv') \
    .load(s3_filepath)

df = df.withColumn('EffectiveDateTime', to_timestamp("EffectiveDate"))
df = df.drop("EffectiveDate")
df.printSchema()

spark.sql("DROP TABLE IF EXISTS {}.ccn_aaos_endpoint PURGE".format('default'))

df.registerTempTable("df")

spark.sql("""
    CREATE TABLE {}.ccn_aaos_endpoint
    USING PARQUET
    LOCATION '{}/pinpoint-data-segment-aos-athena'
    AS SELECT * FROM df
""".format('default', 's3://ccn-alerts-104328-psp-ade-relay'))

print('JOB Completed!!!')
job.commit()
