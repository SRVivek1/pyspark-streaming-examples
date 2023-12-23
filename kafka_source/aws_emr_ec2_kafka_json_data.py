"""
    Requirement
    -------------
        >> Read data JSON data from kafka stream
        >> Add watermark
        >> Transform the data
        >> Write it on console.

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import from_json, col, to_timestamp
import os.path
import yaml


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" pyspark-shell'
    )

    spark = SparkSession.builder \
        .appName('spark with kafka') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # read application config
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf = yaml.load(open(cur_dir + '/../' + 'application.yml'), Loader=yaml.FullLoader)
    secrets = yaml.load(open(cur_dir + '/../' + '.secrets'), Loader=yaml.FullLoader)

    # AWS config
    aws_access = secrets['AWS_ACCESS']
    hdp_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hdp_conf.set('fs.s3a.access.key', aws_access['ACCESS_KEY'])
    hdp_conf.set('fs.s3a.secret.key', aws_access['SECRET_KEY'])

    # Read stream from kafka
    kafka_conf = app_conf['KAFKA_CONF']
    input_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_conf['SERVER'] + ':9092') \
        .option('subscribe', kafka_conf['TOPIC'])\
        .load()

    # Watermark in order to handle late arriving data. Spark’s engine automatically tracks the current event time
    # and can filter out incoming messages if they are older than time T
    data_stream_transformed = input_df \
        .withWatermark(eventTime='timestamp', delayThreshold='1 day')

    # Input stream json
    # {"firstName": "Quentin", "lastName": "Corkery", "birthDate": "1984-10-26T03:52:14.449+0000"}
    # {"firstName": "Neil", "lastName": "Macejkovic", "birthDate": "1971-08-06T18:03:11.533+0000"}
    person_schema = StructType([
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("birthDate", StringType(), True)])

    # extract data from json
    data_stream_transformed = data_stream_transformed \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), person_schema).alias("person")) \
        .select("person.firstName", "person.lastName", "person.birthDate") \
        .withColumn("birthDate", to_timestamp("birthDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

    # Write data to console
    data_stream_transformed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .start() \
        .awaitTermination()

#
# command
# ------------
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" --master yarn ./program.py
#
# Environment
# -------------
# 1. AWS EMR Cluster
# 2. Kafka running on EC2 (1 cpu, 2 GiB of RAM, 8 GB Storage)
#
# Output
# ---------------
# Not captured
#
#
