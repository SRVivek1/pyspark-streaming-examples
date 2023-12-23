"""
    Requirement
    ---------------
        This application demonstrates use of Spark streaming API to read data streams from
        AWS S3.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import sum, desc
import os
import yaml

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('POC - Spark streaming from aws s3') \
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

    # schema
    schema = StructType() \
        .add('city_code', StringType(), True) \
        .add('city', StringType(), True) \
        .add('major_category', StringType(), True) \
        .add('minor_category', StringType(), True) \
        .add('value', StringType(), True) \
        .add('year', StringType(), True) \
        .add('month', StringType(), True)

    s3_conf = app_conf['AWS_S3']
    data_path = 's3a://' + s3_conf['READ_BUCKET'] + '/' + s3_conf['DROP_LOCATION']
    raw_crime_df = spark.readStream \
        .option('header', 'false') \
        .option('maxFilesPerTrigger', 2) \
        .schema(schema) \
        .csv(data_path)

    raw_crime_df.printSchema()

    # Create a temp table in memory
    raw_crime_df.createOrReplaceTempView('CrimeData')

    # filter using sql query
    category_df = spark.sql("SELECT major_category, value FROM CrimeData WHERE year = '2016'")

    # Streaming readiness
    print(f"Is the stream ready : {raw_crime_df.isStreaming}")

    crime_per_cat_df = category_df.groupBy("major_category") \
        .agg(sum("value").alias("convictions")) \
        .orderBy(desc("convictions"))

    query = crime_per_cat_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .start() \
        .awaitTermination()

#
# Command
# -----------------
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" --master yarn ./program.py
#
# Platform - AWS ERM
# ------------------------
#
#
# Output
# ------------
#
#
#
