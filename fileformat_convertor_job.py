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

csvFiles = ['/cif-to-guid/',
            '/cupid-to-guid/',
            '/sfmc-data/'
            ]

for fileName in csvFiles:
    filePath = s3Location + '/input' + fileName
    print("File Name: ", filePath)

    if 'sfmc' in fileName:
        fileDelimiter = '|'
    else:
        fileDelimiter = ','

    df = spark.read.options(header = 'True', delimiter = fileDelimiter, inferSchema = 'False') \
        .format('csv') \
        .load(filePath)

    df.printSchema()
    print("{} count: {}".format(fileName, df.count()))
    #df.limit(10).show(truncate=False)gzip /Users/gz899p/Downloads/SFMC-ONETIME-MOB-CONTACT-PREF-20220318010101.dat

    outputFolder = fileName.replace(".dat","").replace(".csv","")
    df.write.mode("overwrite") \
        .option("compression", "snappy")\
        .parquet(s3Location + '/output' + outputFolder)

    df2 = spark.read.parquet(s3Location + '/output' + outputFolder)
    df2.printSchema()
    #df2.limit(10).show(truncate=False)

print('JOB Completed!!!')
job.commit()