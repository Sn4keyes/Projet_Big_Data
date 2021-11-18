#!/usr/bin/python

from kafka import KafkaConsumer
import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType 
from datetime import datetime
from pandas.tseries import offsets
import pandas as pd
import json
import time


list_crypto = ['bitcoin', 'ethereum', 'binancecoin', 'tether', 'solana', 'cardano', 'polkadot', 'dogecoin', 'litecoin']
BROKER = 'kafka:9093'
TOPIC = 'crypto1'

def process_msg(msg, spark, post_crypto):
    df = pd.DataFrame(json.loads(msg.value))
    spark_df = spark.createDataFrame(df)
    spark_df.show()
    print(msg.offset)
    message = json.loads(msg.value)
    print(message)
    post_crypto.insert_one(message).inserted_id

def spar_connect():
    spark = SparkSession    \
            .builder    \
            .master('local')    \
            .appName('Crypto')  \
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
        process_msg(msg, spark, post_crypto)