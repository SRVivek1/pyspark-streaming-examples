"""
    Requirement
    -------------
        >> Read data from kafka stream and write it on console.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, count
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

    # Do the required transformation

    # Write data to console
    # Note:
    # Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
    #
    checkpointLocation = 's3a://' + app_conf['AWS_S3']['WRITE_BUCKET'] + '/' + kafka_conf['KAFKA_CHECKPOINT_LOCATION']
    streaming_query = input_df \
        .selectExpr('CAST(value as STRING)') \
        .withColumn("value", split("value", " ")) \
        .withColumn("value", explode("value")) \
        .groupBy("value") \
        .agg(count("value")) \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('checkpointLocation', checkpointLocation) \
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
# Sample Input
# ---------------
# ubuntu@ip-172-31-29-141:~/kafka_2.12-2.2.0$ sudo bin/kafka-console-producer.sh --broker-list ec2-34-243-68-20.eu-west-1.compute.amazonaws.com:9092 --topic testmq
# >test
# >test 2
# >test 3
# >test 4
# >test 5
# >hello
# >world
# >hello
# >india
# >india
# >is
# >a
# >asian
# >country
# >it's
# >democratic
#
#
# Output
# ---------------
# -------------------------------------------
# Batch: 0
# -------------------------------------------
# +-----+------------+
# |value|count(value)|
# +-----+------------+
# +-----+------------+
#
# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +-----+------------+
# |value|count(value)|
# +-----+------------+
# |    3|           1|
# |    5|           1|
# |    4|           1|
# | test|           5|
# |    2|           1|
# +-----+------------+
#
# -------------------------------------------
# Batch: 2
# -------------------------------------------
# +-----+------------+
# |value|count(value)|
# +-----+------------+
# |    3|           1|
# |hello|           1|
# |    5|           1|
# |    4|           1|
# | test|           5|
# |    2|           1|
# +-----+------------+
#
# -------------------------------------------
# Batch: 3
# -------------------------------------------
# +-------+------------+
# |  value|count(value)|
# +-------+------------+
# |      3|           1|
# |   it's|           1|
# |  india|           2|
# |  hello|           2|
# |      5|           1|
# |  asian|           1|
# |     is|           1|
# |country|           1|
# |  world|           1|
# |      4|           1|
# |      a|           1|
# |   test|           5|
# |      2|           1|
# +-------+------------+
#
# -------------------------------------------
# Batch: 4
# -------------------------------------------
# +----------+------------+
# |     value|count(value)|
# +----------+------------+
# |         3|           1|
# |      it's|           1|
# |     india|           2|
# |     hello|           2|
# |         5|           1|
# |     asian|           1|
# |        is|           1|
# |   country|           1|
# |democratic|           1|
# |     world|           1|
# |         4|           1|
# |         a|           1|
# |      test|           5|
# |         2|           1|
# +----------+------------+
#
# -------------------------------------------
# Batch: 5
# -------------------------------------------
# +----------+------------+
# |     value|count(value)|
# +----------+------------+
# |         3|           1|
# |      it's|           1|
# |     india|           2|
# |     hello|           2|
# |         5|           1|
# |     asian|           1|
# |        is|           1|
# |   country|           1|
# |democratic|           1|
# |     world|           1|
# |         4|           1|
# |   America|           1|
# |         a|           1|
# |      test|           5|
# |         2|           1|
# +----------+------------+
#
