import sys
import datetime
import base64
import decimal
import boto3
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, \
                            ['JOB_NAME', \
                            'aws_region', \
                            'checkpoint_location', \
                            'dynamodb_sink_table', \
                            'dynamodb_static_table'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read parameters
checkpoint_location = args['checkpoint_location']
aws_region = args['aws_region']

# DynamoDB config
dynamodb_sink_table = args['dynamodb_sink_table']
dynamodb_static_table = args['dynamodb_static_table']

def write_to_dynamodb(row):
    '''
    Add row to DynamoDB.
    '''
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    start = str(row['window'].start)
    end = str(row['window'].end)
    dynamodb.Table(dynamodb_sink_table).put_item(
      Item = { 'userid': row['userid'], \
                'channelid':row['channelid'], \
                'genre': str(row['genre']), \
                'lastactive': str(row['genre']), \
                'title': str(row['title']), \
                'watchfrequency': row['watchfrequency'], \
                'etags': row['etags'] })

#

dynamodb_dynamic_frame = glueContext.create_dynamic_frame.from_options( \
    "dynamodb", \
    connection_options={
    "dynamodb.input.tableName": dynamodb_static_table,
    "dynamodb.throughput.read.percent": "1.5"
  }
)

dynamodb_lookup_df = dynamodb_dynamic_frame.toDF().cache()

# Read from Kinesis Data Stream
streaming_data = spark.readStream \
                    .format("kinesis") \
                    .option("streamName","bibhusha-demo-datastream") \
                    .option("endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
                    .option("startingPosition", "TRIM_HORIZON") \
                    .load()

# Retrieve Sensor columns and simple projection
netflix_data = streaming_data \
    .select(from_json(col("data") \
    .cast("string"),glueContext.get_catalog_schema_as_spark_schema("bibhushadb","bibhusha-demotable")) \
    .alias("netflixdata")) \
    .select("netflixdata.*") \
    .withColumn("lastactive", to_timestamp(col('lastactive'), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("lastactive", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

# # Stream static join, ETL to augment with favorite
netflix_joined_df = netflix_data.join(dynamodb_lookup_df, "userid") \
    .withColumn('impression', when( \
    ((netflix_data.watchfrequency < 3)), "neutral") \
    .when( \
    ((netflix_data.watchfrequency >= 3) |
    (netflix_data.watchfrequency <= 10)), "like") \
    .otherwise("favorite"))


netflix_joined_df.printSchema()

# Drop some values
netflix_transformed_df = netflix_joined_df \
                            .drop('etags')

netflix_transformed_df.printSchema()

netflix_df = netflix_transformed_df \
    .groupBy(window(col('lastactive'), '10 minute', '5 minute'), \
    netflix_transformed_df.impression, netflix_transformed_df.userid)
    

netflix_df.printSchema()

# Write to DynamoDB sink
netflix_query = netflix_df \
    .writeStream \
    .foreach(write_to_dynamodb) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .start()

netflix_query.awaitTermination()

job.commit()