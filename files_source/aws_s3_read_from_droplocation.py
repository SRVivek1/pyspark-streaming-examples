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
    # Note : Below code added after output was captured - execute again for update result
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
# -------------------------------------------
# Batch: 0
# -------------------------------------------
# +---------+----------------------+---------------------------+--------------------------------------+-----+----+-----+
# |city_code|city                  |major_category             |minor_category                        |value|year|month|
# +---------+----------------------+---------------------------+--------------------------------------+-----+----+-----+
# |E01000896|Camden                |Criminal Damage            |Criminal Damage To Other Building     |0    |2012|5    |
# |E01004602|Wandsworth            |Violence Against the Person|Common Assault                        |0    |2016|3    |
# |E01002641|Hounslow              |Theft and Handling         |Motor Vehicle Interference & Tampering|0    |2011|2    |
# |E01003560|Newham                |Drugs                      |Drug Trafficking                      |0    |2016|3    |
# |E01002491|Hillingdon            |Robbery                    |Personal Property                     |0    |2015|5    |
# |E01002473|Hillingdon            |Theft and Handling         |Theft/Taking of Pedal Cycle           |0    |2012|11   |
# |E01004018|Southwark             |Other Notifiable Offences  |Going Equipped                        |0    |2016|11   |
# |E01004281|Tower Hamlets         |Theft and Handling         |Handling Stolen Goods                 |0    |2008|3    |
# |E01002857|Kensington and Chelsea|Violence Against the Person|Harassment                            |0    |2014|1    |
# |E01000195|Barnet                |Theft and Handling         |Theft/Taking Of Motor Vehicle         |0    |2010|1    |
# +---------+----------------------+---------------------------+--------------------------------------+-----+----+-----+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 1
# -------------------------------------------
# +---------+--------------------+---------------------------+--------------------------------------+-----+----+-----+
# |city_code|city                |major_category             |minor_category                        |value|year|month|
# +---------+--------------------+---------------------------+--------------------------------------+-----+----+-----+
# |E01000571|Brent               |Violence Against the Person|Wounding/GBH                          |0    |2011|4    |
# |E01000162|Barnet              |Robbery                    |Personal Property                     |1    |2014|7    |
# |E01002428|Hillingdon          |Theft and Handling         |Motor Vehicle Interference & Tampering|0    |2015|10   |
# |E01001404|Enfield             |Violence Against the Person|Common Assault                        |0    |2016|11   |
# |E01003574|Newham              |Theft and Handling         |Motor Vehicle Interference & Tampering|0    |2014|2    |
# |E01003896|Richmond upon Thames|Robbery                    |Business Property                     |0    |2013|10   |
# |E01001147|Croydon             |Theft and Handling         |Other Theft                           |0    |2012|11   |
# |E01003003|Kingston upon Thames|Criminal Damage            |Criminal Damage To Motor Vehicle      |0    |2016|2    |
# |E01001773|Hackney             |Theft and Handling         |Other Theft                           |3    |2010|2    |
# |E01002414|Hillingdon          |Violence Against the Person|Common Assault                        |0    |2008|1    |
# +---------+--------------------+---------------------------+--------------------------------------+-----+----+-----+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 2
# -------------------------------------------
# +---------+--------------------+---------------------------+---------------------------+-----+----+-----+
# |city_code|city                |major_category             |minor_category             |value|year|month|
# +---------+--------------------+---------------------------+---------------------------+-----+----+-----+
# |E01001770|Hackney             |Other Notifiable Offences  |Going Equipped             |0    |2011|3    |
# |E01002030|Haringey            |Violence Against the Person|Assault with Injury        |0    |2014|11   |
# |E01033486|Islington           |Theft and Handling         |Handling Stolen Goods      |0    |2016|2    |
# |E01003366|Merton              |Theft and Handling         |Other Theft Person         |0    |2015|5    |
# |E01000212|Barnet              |Violence Against the Person|Common Assault             |0    |2009|4    |
# |E01003430|Merton              |Burglary                   |Burglary in Other Buildings|0    |2013|4    |
# |E01002710|Islington           |Robbery                    |Business Property          |0    |2010|5    |
# |E01002530|Hillingdon          |Violence Against the Person|Common Assault             |1    |2008|3    |
# |E01003810|Richmond upon Thames|Burglary                   |Burglary in Other Buildings|0    |2014|7    |
# |E01002694|Islington           |Theft and Handling         |Other Theft Person         |1    |2012|9    |
# +---------+--------------------+---------------------------+---------------------------+-----+----+-----+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 3
# -------------------------------------------
# +---------+--------------------+---------------------------+---------------------------+-----+----+-----+
# |city_code|city                |major_category             |minor_category             |value|year|month|
# +---------+--------------------+---------------------------+---------------------------+-----+----+-----+
# |E01032743|Ealing              |Theft and Handling         |Theft/Taking of Pedal Cycle|0    |2008|2    |
# |E01004624|Wandsworth          |Violence Against the Person|Wounding/GBH               |0    |2013|10   |
# |E01004413|Waltham Forest      |Violence Against the Person|Other violence             |0    |2014|10   |
# |E01001647|Greenwich           |Robbery                    |Personal Property          |1    |2008|2    |
# |E01000664|Bromley             |Theft and Handling         |Other Theft                |0    |2012|7    |
# |E01000064|Barking and Dagenham|Other Notifiable Offences  |Other Notifiable           |0    |2015|8    |
# |E01001526|Enfield             |Other Notifiable Offences  |Other Notifiable           |0    |2008|12   |
# |E01004500|Wandsworth          |Burglary                   |Burglary in Other Buildings|0    |2008|8    |
# |E01000718|Bromley             |Violence Against the Person|Harassment                 |2    |2011|6    |
# |E01000294|Barnet              |Criminal Damage            |Other Criminal Damage      |0    |2008|10   |
# +---------+--------------------+---------------------------+---------------------------+-----+----+-----+
# only showing top 10 rows
#
# -------------------------------------------
# Batch: 4
# -------------------------------------------
# +---------+----------------------+---------------------------+---------------------------------+-----+----+-----+
# |city_code|city                  |major_category             |minor_category                   |value|year|month|
# +---------+----------------------+---------------------------+---------------------------------+-----+----+-----+
# |E01002837|Kensington and Chelsea|Criminal Damage            |Criminal Damage To Dwelling      |0    |2016|2    |
# |E01001107|Croydon               |Drugs                      |Possession Of Drugs              |0    |2014|11   |
# |E01004136|Sutton                |Violence Against the Person|Offensive Weapon                 |0    |2015|7    |
# |E01003247|Lewisham              |Theft and Handling         |Theft/Taking of Pedal Cycle      |0    |2013|4    |
# |E01000836|Bromley               |Theft and Handling         |Theft From Motor Vehicle         |3    |2014|5    |
# |E01001798|Hackney               |Criminal Damage            |Other Criminal Damage            |1    |2011|8    |
# |E01004183|Sutton                |Criminal Damage            |Criminal Damage To Other Building|0    |2014|12   |
# |E01000666|Bromley               |Burglary                   |Burglary in a Dwelling           |0    |2011|4    |
# |E01001444|Enfield               |Violence Against the Person|Offensive Weapon                 |0    |2011|3    |
# |E01002692|Hounslow              |Burglary                   |Burglary in Other Buildings      |1    |2011|8    |
# +---------+----------------------+---------------------------+---------------------------------+-----+----+-----+
# only showing top 10 rows
#
#
