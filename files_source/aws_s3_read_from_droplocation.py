"""
    Requirement
    ---------------
        This application demonstrates use of Spark streaming API to read data streams from
        AWS S3.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
import os
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

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

    # Streaming readiness
    print(f"Is the stream ready : {raw_crime_df.isStreaming}")

    # do the required transformations
    crime_df = raw_crime_df \
        .select("city", "year", "month", "value") \
        .withColumnRenamed("value", "convictions")

    # Write the data to console
    query = crime_df.writeStream \
        .outputMode('append') \
        .option('truncate', 'false') \
        .option('numRows', 10) \
        .format('console') \
        .start() \
        .awaitTermination()

#
# command
# -------------------
# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" --master yarn ./program.py
#
# Environment
# ----------------
# 1. AWS EC2 - Kafka server
# 2. AWS EMR (running 1 test case)
#
# Output
# ----------------
# root
#  |-- city_code: string (nullable = true)
#  |-- city: string (nullable = true)
#  |-- major_category: string (nullable = true)
#  |-- minor_category: string (nullable = true)
#  |-- value: string (nullable = true)
#  |-- year: string (nullable = true)
#  |-- month: string (nullable = true)
#
# Is the stream ready : True
# -------------------------------------------
# Batch: 0
# -------------------------------------------
# +----------------------+----+-----+-----------+
# |city                  |year|month|convictions|
# +----------------------+----+-----+-----------+
# |Camden                |2012|5    |0          |
# |Wandsworth            |2016|3    |0          |
# |Hounslow              |2011|2    |0          |
# |Newham                |2016|3    |0          |
# |Hillingdon            |2015|5    |0          |
# |Hillingdon            |2012|11   |0          |
# |Southwark             |2016|11   |0          |
# |Tower Hamlets         |2008|3    |0          |
# |Kensington and Chelsea|2014|1    |0          |
# |Barnet                |2010|1    |0          |
# +----------------------+----+-----+-----------+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +--------------------+----+-----+-----------+
# |city                |year|month|convictions|
# +--------------------+----+-----+-----------+
# |Brent               |2011|4    |0          |
# |Barnet              |2014|7    |1          |
# |Hillingdon          |2015|10   |0          |
# |Enfield             |2016|11   |0          |
# |Newham              |2014|2    |0          |
# |Richmond upon Thames|2013|10   |0          |
# |Croydon             |2012|11   |0          |
# |Kingston upon Thames|2016|2    |0          |
# |Hackney             |2010|2    |3          |
# |Hillingdon          |2008|1    |0          |
# +--------------------+----+-----+-----------+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 2
# -------------------------------------------
# +--------------------+----+-----+-----------+
# |city                |year|month|convictions|
# +--------------------+----+-----+-----------+
# |Hounslow            |2014|10   |0          |
# |Hackney             |2010|1    |0          |
# |Islington           |2014|9    |0          |
# |Sutton              |2014|12   |0          |
# |Camden              |2010|7    |0          |
# |Redbridge           |2013|8    |0          |
# |Bexley              |2012|1    |0          |
# |Ealing              |2009|9    |0          |
# |Haringey            |2010|1    |1          |
# |Richmond upon Thames|2016|3    |1          |
# +--------------------+----+-----+-----------+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 3
# -------------------------------------------
# +----------------------+----+-----+-----------+
# |city                  |year|month|convictions|
# +----------------------+----+-----+-----------+
# |Kensington and Chelsea|2016|2    |0          |
# |Croydon               |2014|11   |0          |
# |Sutton                |2015|7    |0          |
# |Lewisham              |2013|4    |0          |
# |Bromley               |2014|5    |3          |
# |Hackney               |2011|8    |1          |
# |Sutton                |2014|12   |0          |
# |Bromley               |2011|4    |0          |
# |Enfield               |2011|3    |0          |
# |Hounslow              |2011|8    |1          |
# +----------------------+----+-----+-----------+
# only showing top 10 rows
#
#
