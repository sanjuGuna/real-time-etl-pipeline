import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType, StructType

def create_spark_connection(): #create spark conn
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("sparkStreaming") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info("spark connection created successfully!")
    except Exception as e:
        logging.error(f"Could not create spark connection due to {e}.")

    return s_conn

def connect_to_kafka(spark_conn): #connect to kafka topics ,takes the json data and continue to process the data
    # spark_df = None
    #try:
        return spark_conn.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers','broker:29092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
        #logging.info(f"Kafka dataframe created successfully!")
    # except Exception as e:
    #     logging.warning(f"kafka dataframe could not be created  due to {e}.")
    # return spark_df


def create_structured_df_from_kafka(spark_df): # structuring in the way to store in cassandra Db
    schema = StructType([
        StructField('user_id', StringType(), False),
        StructField('firstname', StringType(), False),
        StructField('lastname', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('post_code', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('dob', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False),
    ])

    selective_df = spark_df.selectExpr("CAST(value AS STRING)") \
          .select(from_json(col('value'), schema).alias("data")).select("data.*")

    print(selective_df)

    return selective_df


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    kafka_df = connect_to_kafka(spark_conn)
    selection_df = create_structured_df_from_kafka(kafka_df)
    selection_df = selection_df.filter(col("user_id").isNotNull())
    streaming_query= (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("spark.cassandra.connection.host", "cassandra")
                .option('checkpointLocation', "/opt/spark/checkpoints")
                .option('keyspace', 'spark_streams')
                .option('table', 'created_users')
                .option("queryName", "kafka_to_cassandra_users")
                .start())

    streaming_query.awaitTermination()