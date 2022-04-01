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
from datetime import date
import os

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

s3_resource = boto3.resource('s3')

today = date.today()
today = today.strftime("%d_%m_%Y")


archive_input_path = ['/sfmc-data/', '/cupid-to-guid/', '/cif-to-guid/']

for path in archive_input_path:
    src = s3Location+''+'/input' + path
    dest = s3Location+''+'/input/'+ today + path
    cmd = 'aws s3 mv {} {} --recursive'.format(src, dest)
    print('cmd: '+cmd)
    print(os.system(cmd))

archive_final_path = ['/data-load/', '/error-files/sfmc_customers_not_found_in_acm/', '/error-files/sfmc_customers_not_found_in_ccn/']

for path in archive_final_path:
    src = s3Location+''+'/final' + path
    dest = s3Location+''+'/final/'+ today + path
    cmd = 'aws s3 mv {} {} --recursive'.format(src, dest)
    print('cmd: '+cmd)
    print(os.system(cmd))

archive_output_path = ['/ccn-data/']

for path in archive_output_path:
    src = s3Location+''+'/output' + path
    dest = s3Location+''+'/output/'+ today + path
    cmd = 'aws s3 mv {} {} --recursive'.format(src, dest)
    print('cmd: '+cmd)
    print(os.system(cmd))

print('JOB Completed!!!')
job.commit()
