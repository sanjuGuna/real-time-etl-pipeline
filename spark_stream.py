import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType, StructType


def create_keyspace(session): #highest level of container or more similar to tables in RDBMS create keyspace
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                    """)

    print("Keyspace created successfully!!")

def create_table(session):#create table
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                    user_id TEXT PRIMARY KEY,
                    firstName TEXT,
                    lastName TEXT,
                    gender TEXT,
                    address TEXT,
                    post_code TEXT,
                    email TEXT,
                    username TEXT,
                    registered_date TEXT,
                    phone TEXT,
                    picture TEXT);
                    """)
    print("Table created successfully!!")

def insert_data(session, **kwargs): #insert data
    print("Inserting data into table...")
    user_id= kwargs.get('user_id')
    firstName = kwargs.get('firstName')
    lastName = kwargs.get('lastName')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob =kwargs.get('dob')
    registered_date =kwargs.get('registered_date')
    phone= kwargs.get('phone')
    picture =kwargs.get('picture')

    try:
        session.execute("""
                        INSERT INTO spark_streams.created_users(user_id, firstName, lastName,
                        gender, address, post_code, email, username, dob, registered_date, phone, picture)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,(user_id, firstName, lastName, gender, address, post_code, email, username, dob,
                             registered_date, phone, picture))
        logging.info(f"Data inserted for {firstName} {lastName}!")
    except Exception as e:
        logging.error(f"Error inserting data due to {e}")

def create_spark_connection(): #create spark conn
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("sparkStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info("spark connection created successfully!")
    except Exception as e:
        logging.error(f"Could not create spark connection due to {e}.")

    return s_conn


def create_cassandra_connection(): # create cassandra conn
    try:
        cluster = Cluster(['cassandra'])# connection to cassandra cluster
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not connect to cassandra cluster due to {e}.")
        return None


def create_structured_df_from_kafka(spark_df): # structuring in the way to store in cassandra Db
    schema = StructType([
        StructField('user_id', StringType(), False),
        StructField('firstName', StringType(), False),
        StructField('lastName', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('post_code', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False),
    ])

    selective_df = spark_df.selectExpr("CAST(value AS STRING)") \
          .select(from_json(col('value'), schema).alias("data")).select("data.*")

    print(selective_df)

    return selective_df

def connect_to_kafka(spark_conn): #connect to kafka topics ,takes the json data and continue to process the data
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers','broker:9092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
        logging.info(f"Kafka dataframe created successfully!")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created  due to {e}.")
    return spark_df

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df =  connect_to_kafka(spark_conn) #connects to kafka with spark_conn and gets the data
        selection_df = create_structured_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            #insert_data(session) we are not doing in once, we are streaming the data..

            streaming_query= (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                            .option('checkpointLocation', "/tmp/checkpoint")
                            .option('keyspace', 'spark_streams')
                            .option('table', 'created_users')
                            .start())

            streaming_query.awaitTermination()