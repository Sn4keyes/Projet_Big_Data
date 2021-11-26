#!/usr/bin/python

from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pandas.tseries import offsets
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import pymongo
import json
import time

BROKER = 'kafka:9093'
TOPIC = 'crypto5'
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

def post_in_bdd(spark_df):
    spark_df.write.format("mongo").mode("append").option("database","crypto").option("collection", "crypto_col").save()

def tot_all_crypto(spark_df):
    print("\n########## Total all market cap crypto : ##########")
    Moyenne_BTC_market_cap = spark_df.agg({'BTC_market_cap': 'mean'}).show()
    Moyenne_ETH_market_cap = spark_df.agg({'ETH_market_cap': 'mean'}).show()
    Moyenne_BNB_market_cap = spark_df.agg({'BNB_market_cap': 'mean'}).show()
    Moyenne_USDT_market_cap = spark_df.agg({'USDT_market_cap': 'mean'}).show()
    Moyenne_SOL_market_cap = spark_df.agg({'SOL_market_cap': 'mean'}).show()
    Moyenne_ADA_market_cap = spark_df.agg({'ADA_market_cap': 'mean'}).show()
    Moyenne_DOT_market_cap = spark_df.agg({'DOT_market_cap': 'mean'}).show()
    Moyenne_DOGE_market_cap = spark_df.agg({'DOGE_market_cap': 'mean'}).show()
    Moyenne_LTC_market_cap = spark_df.agg({'LTC_market_cap': 'mean'}).show()
    # notre_cap_tot = Moyenne_BTC_market_cap.collect() + Moyenne_ETH_market_cap.collect() + Moyenne_BNB_market_cap.collect() + Moyenne_USDT_market_cap.collect() + Moyenne_SOL_market_cap.collect() + Moyenne_ADA_market_cap.collect() + Moyenne_DOT_market_cap.collect() + Moyenne_DOGE_market_cap.collect() + Moyenne_LTC_market_cap.collect()
    # print(notre_cap_tot)

def corr_btc_market_cap_tot_vol(spark_df):
    print("\n########## Correlation crypto market cap --> total volume : ##########")
    Corr_Cap_BTC = spark_df.stat.corr("BTC_market_cap", "BTC_total_vol")
    print("BTC = ", Corr_Cap_BTC)
    Corr_Cap_ETH = spark_df.stat.corr("ETH_market_cap", "ETH_total_vol")
    print("ETH = ", Corr_Cap_ETH)
    Corr_Cap_BNB = spark_df.stat.corr("BNB_market_cap", "BNB_total_vol")
    print("BNB = ", Corr_Cap_BNB)
    Corr_Cap_USDT = spark_df.stat.corr("USDT_market_cap", "USDT_total_vol")
    print("USDT = ", Corr_Cap_USDT)
    Corr_Cap_SOL = spark_df.stat.corr("SOL_market_cap", "SOL_total_vol")
    print("SOL = ", Corr_Cap_SOL)
    Corr_Cap_ADA = spark_df.stat.corr("ADA_market_cap", "ADA_total_vol")
    print("ADA = ", Corr_Cap_ADA)
    Corr_Cap_DOT = spark_df.stat.corr("DOT_market_cap", "DOT_total_vol")
    print("DOT = ", Corr_Cap_DOT)
    Corr_Cap_DOGE = spark_df.stat.corr("DOGE_market_cap", "DOGE_total_vol")
    print("DOGE = ", Corr_Cap_DOGE)
    Corr_Cap_LTC = spark_df.stat.corr("LTC_market_cap", "LTC_total_vol")
    print("LTC = ", Corr_Cap_LTC)


def corr_btc_price(spark_df):
    print("########## Correlation BTC --> all crypto : ##########")
    Corr_BTC = spark_df.stat.corr("BTC_prices", "BTC_prices")
    print("BTC -> BTC = ", Corr_BTC)
    Corr_BTC_ETH = spark_df.stat.corr("BTC_prices", "ETH_prices")
    print("BTC -> ETH = ", Corr_BTC_ETH)
    Corr_BTC_BNB = spark_df.stat.corr("BTC_prices", "BNB_prices")
    print("BTC -> BNB = ", Corr_BTC_BNB)
    Corr_BTC_USDT = spark_df.stat.corr("BTC_prices", "USDT_prices")
    print("BTC -> USDT = ", Corr_BTC_USDT)
    Corr_BTC_SOL = spark_df.stat.corr("BTC_prices", "SOL_prices")
    print("BTC -> SOL = ", Corr_BTC_SOL)
    Corr_BTC_ADA = spark_df.stat.corr("BTC_prices", "ADA_prices")
    print("BTC -> ADA = ", Corr_BTC_ADA)
    Corr_BTC_DOT = spark_df.stat.corr("BTC_prices", "DOT_prices")
    print("BTC -> DOT = ", Corr_BTC_DOT)
    Corr_BTC_DOGE = spark_df.stat.corr("BTC_prices", "DOGE_prices")
    print("BTC -> DOGE = ", Corr_BTC_DOGE)
    Corr_BTC_LTC = spark_df.stat.corr("BTC_prices", "LTC_prices")
    print("BTC -> LTC = ", Corr_BTC_LTC)

def minimum(spark_df):
    print("########## Minimum : ##########")
    min_BTC = spark_df.agg({"BTC_prices": "min"}).show()
    min_ETH = spark_df.agg({"ETH_prices": "min"}).show()
    min_BNB = spark_df.agg({"BNB_prices": "min"}).show()
    min_USDT = spark_df.agg({"USDT_prices": "min"}).show()
    min_SOL = spark_df.agg({"SOL_prices": "min"}).show()
    min_ADA = spark_df.agg({"ADA_prices": "min"}).show()
    min_DOT = spark_df.agg({"DOT_prices": "min"}).show()
    min_DOGE = spark_df.agg({"DOGE_prices": "min"}).show()
    min_LTC = spark_df.agg({"LTC_prices": "min"}).show()

def maximum(spark_df):
    print("########## Maximum : ##########")
    max_BTC = spark_df.agg({"BTC_prices": "max"}).show()
    max_ETH = spark_df.agg({"ETH_prices": "max"}).show()
    max_BNB = spark_df.agg({"BNB_prices": "max"}).show()
    max_USDT = spark_df.agg({"USDT_prices": "max"}).show()
    max_SOL = spark_df.agg({"SOL_prices": "max"}).show()
    max_ADA = spark_df.agg({"ADA_prices": "max"}).show()
    max_DOT = spark_df.agg({"DOT_prices": "max"}).show()
    max_DOGE = spark_df.agg({"DOGE_prices": "max"}).show()
    max_LTC = spark_df.agg({"LTC_prices": "max"}).show()

def average(spark_df):
    print("########## Average : ##########")
    Moyenne_BTC = spark_df.agg({'BTC_prices': 'mean'}).show()
    Moyenne_ETH = spark_df.agg({'ETH_prices': 'mean'}).show()
    Moyenne_BNB = spark_df.agg({'BNB_prices': 'mean'}).show()
    Moyenne_USDT = spark_df.agg({'USDT_prices': 'mean'}).show()
    Moyenne_SOL = spark_df.agg({'SOL_prices': 'mean'}).show()
    Moyenne_ADA = spark_df.agg({'ADA_prices': 'mean'}).show()
    Moyenne_DOT = spark_df.agg({'DOT_prices': 'mean'}).show()
    Moyenne_DOGE = spark_df.agg({'DOGE_prices': 'mean'}).show()
    Moyenne_LTC = spark_df.agg({'LTC_prices': 'mean'}).show()

def spark_connect():
    spark = SparkSession    \
            .builder    \
            .master('local')    \
            .appName('Crypto')  \
            .config("spark.mongodb.input.uri", "mongodb://root:root@mongo:27017/crypto.crypto_col?authSource=admin")  \
            .config("spark.mongodb.output.uri", "mongodb://root:root@mongo:27017/crypto.crypto_col?authSource=admin") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
            .config("spark-jars-packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")  \
            .getOrCreate()
    spark.sparkContext
    return spark

if __name__ == "__main__":

    try:
        client = MongoClient('mongo', 27017, username = 'root', password = 'root')
        db_crypto = client.crypto
        crypto_col = db_crypto.posts
        crypto_col.drop()
        print("########## Création de la base de données ##########")
    except:
        print("Erreur de connexion à MongoDB")
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BROKER], api_version=(2,6,0))
    spark = spark_connect()
    for msg in consumer:
        print("########## Received Data From Producer : OK ##########")
        df = pd.DataFrame.from_dict(json.loads(msg.value))
        print("\n########## DataFrame Pandas : ##########\n")
        print(df)
        spark_df = spark.createDataFrame(df)
        post_in_bdd(spark_df)
        print("\n########## DataFrame Spark : ##########\n")
        spark_df.show(5, False)
        average(spark_df)
        maximum(spark_df)
        minimum(spark_df)
        corr_btc_price(spark_df)
        corr_btc_market_cap_tot_vol(spark_df)
        tot_all_crypto(spark_df)