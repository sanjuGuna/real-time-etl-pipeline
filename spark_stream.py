import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqlengine.connection import session
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session): #highest level of container or more similar to tables in RDBMS create keyspace
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                    """)

    print("Keyspace created successfully!!")

def create_table(session):#create table
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.create_users (
                    id UUID PRIMARY KEY,
                    firstName TEXT,
                    lastName TEXT,
                    gender TEXT,
                    post_code TEXT,
                    email TEXT,
                    username TEXT,
                    registered_date TEXT,
                    phone TEXT,
                    picture TEXT,
                    """)
    print("Table created successfully!!")

def insert_data(session, **kwargs):
    #insert data

def create_spark_connection(): #create spark conn
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("sparkStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                    "org.apache.spark:spark:spark-sql-kafka-0-10_2.13:3.5.2") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info("spark connection created successfully!")
    except Exception as e:
        logging.error(f"Could not create spark connection due to {e}.")

    return s_conn
def create_cassandra_connection(): # create cassandra conn
    try:
        cluster = Cluster(['localhost'])# connection to cassandra cluster
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not connect to cassandra cluster due to {e}.")
        return None


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
