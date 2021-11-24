#!/usr/bin/python

from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
from pyspark.sql import SparkSession
from pymongo import MongoClient
from kafka import KafkaConsumer
from datetime import datetime
from pandas.tseries import offsets
import pymongo
import pandas as pd
import json
import time

BROKER = 'kafka:9093'
TOPIC = 'crypto3'
list_crypto = [
    'bitcoin',
    'ethereum',
    'binancecoin',
    'tether',
    'solana',
    'cardano',
    'polkadot',
    'dogecoin',
    'litecoin'
]

def post_in_bdd(msg, post_crypto):
    message = json.loads(msg)
    post_crypto.insert_one(message).inserted_id

def spar_connect():
    spark = SparkSession    \
            .builder    \
            .master('local')    \
            .appName('Crypto')  \
            .config("spark.mongodb.input.uri", "mongodb://mongo:27017/database.*")  \
            .config("spark.mongodb.output.uri", "mongodb://mongo:27017/database.*") \
            .config("spark-jars-packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")  \
            .getOrCreate()
    spark.sparkContext
    return spark

if __name__ == "__main__":

    try:
        client = MongoClient('mongo', 27017, username = 'root', password = 'root')
        db_crypto = client.crypto
        post_crypto = db_crypto.posts
        post_crypto.drop()
        print("Création de la base de données")
    except:
        print("Erreur de connexion à MongoDB")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    spark = spar_connect()
    for msg in consumer:
        print("########## Received Data From Producer : OK ##########")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("\nDataFrame Pandas :\n")
        print(df)
        spark_df = spark.createDataFrame(df)
        print("\nDataFrame Spark :\n")
        spark_df.show(5, False)
        # post_in_bdd(spark_df, post_crypto)