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
                            'dynamodb_sink_table'])

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
# dynamodb_static_table = args['dynamodb_static_table']

def write_to_dynamodb(row):
    # Add row to DynamoDB.
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    dynamodb.Table(dynamodb_sink_table).put_item(
        Item = { 'userid': row['userid'], \
                'channelid': row['channelid '], \
                'genre': str(row['genre']), \
                'lastactive': str(row['lastactive']), \
                'title': str(row['title']), \
                'watchfrequency': row['watchfrequency'], \
                'etags': row['etags']})
        
def write_to_dynamodb(row):
    dynamodb = glueContext.create_dynamic_frame.fromDF(row, glueContext, "write_to_dynamodb")
    dynamodb.toDF().write \
        .format("dynamodb") \
        .option("tableName", dynamodb_sink_table) \
        .option("region", aws_region) \
        .mode("Append") \
        .save()

# Read from DynamoDB as a DataFrame
dynamodb_dynamic_frame = glueContext.create_dynamic_frame.from_options( \
    "dynamodb", \
    connection_options={
    "dynamodb.input.tableName": dynamodb_static_table,
    "dynamodb.throughput.read.percent": "1.5"
  }
)


dynamodb_lookup_df = dynamodb_dynamic_frame.toDF().cache()

# Read from Kinesis Data Stream
netflix_data = spark.readStream \
                    .format("kinesis") \
                    .option("streamName","bibhusha-demo-datastream") \
                    .option("endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
                    .option("startingPosition", "TRIM_HORIZON") \
                    .load()

# ETL to augment with 'impression' column
netflix_df = netflix_data.withColumn('impression',
    when(netflix_data['watchfrequency'] < 3, "neutral")
    .when(((netflix_data['watchfrequency'] >= 3) & (netflix_data['watchfrequency'] <= 10)), "like")
    .otherwise("favorite")
)

# Drop the etags values
netflix_transformed_df = netflix_df.drop('etags')

# Write to DynamoDB sink using foreachBatch
netflix_query = netflix_transformed_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.foreachPartition(write_to_dynamodb)) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .start()

netflix_query.awaitTermination()

job.commit()