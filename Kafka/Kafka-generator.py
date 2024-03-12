from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit, regexp_replace, col
from pyspark.sql.types import IntegerType
import socket
import time
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from faker import Faker
import json
from random import seed
from random import randint
from datetime import date, datetime, timedelta
import sys
import boto3

## Variables
BOOTSTRAP_SERVERS = "boot-uuoa1swb.c1.kafka-serverless.us-east-1.amazonaws.com:9098"
REGION = "us-east-1"
TOPIC = "amznbookreviews"

MIN_YEAR = 1996
MAX_YEAR = 2018
MIN_MONTH = 1
MAX_MONTH = 12
MIN_DAY = 12
MAX_DAY = 28
S3_BUCKET = "amzn-customer-reviews-228924278364"
S3_PREFIX = "amzn-customer-reviews-partitioned/category=Books/"

## Get S3 user secrets
client = boto3.client('secretsmanager',region_name=REGION)
response = client.get_secret_value(
    SecretId='s3all'
)
accessJson = json.loads(response['SecretString'])
accessKeyId = accessJson['accessKey']
secretAccessKey = accessJson['secretAccess']

## Start Spark session
spark = SparkSession\
            .builder.appName("generate-kafka-amzn-books")\
            .enableHiveSupport()\
            .getOrCreate()

sc = spark.sparkContext

## Initialize S3a
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

fake = Faker()

def getS3Path():
    #Faker.seed(0)
    REVIEW_YEAR = fake.random_int(min=MIN_YEAR, max=MAX_YEAR)
    REVIEW_MONTH = fake.random_int(min=MIN_MONTH, max=MAX_MONTH)
    REVIEW_DAY = fake.random_int(MIN_DAY, max=MAX_DAY)
    
    S3_PATH = f"s3a://{S3_BUCKET}/{S3_PREFIX}review_year={REVIEW_YEAR}/review_month={REVIEW_MONTH}/review_day={REVIEW_DAY}/"
    
    return S3_PATH

def writeToKafka(df):
    options_write = {
        "kafka.bootstrap.servers":
            BOOTSTRAP_SERVERS,
        "topic":
            TOPIC,
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

    windowSpec = Window.partitionBy("asin").orderBy("reviewTimeTS")
    df = df.withColumn("row_number",row_number().over(windowSpec))
    df = df.withColumn("asin_key", regexp_replace("asin", r'^0+', '').cast(IntegerType())*col("row_number"))
    
    df_output = df \
        .selectExpr("CAST(asin_key AS STRING) AS key",
                    "to_json(struct(*)) AS value")
    
    
    df_output \
        .write \
        .format("kafka") \
        .options(**options_write) \
        .save()

def main():
    S3_PATH = getS3Path()
    
    df = spark.read.parquet(S3_PATH)
    writeToKafka(df)
    time.sleep(15)

if __name__ == "__main__":
    while 1 == 1:
        main()
