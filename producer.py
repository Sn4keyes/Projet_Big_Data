#!/usr/bin/python

from pycoingecko import CoinGeckoAPI
from kafka import KafkaProducer
import numpy as np
import json
import time
import sys

BROKER = 'localhost:9092'
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

def clean_json(json, crypto_name):
    json_clean = {
        crypto_name + "_timestamp": [],
        crypto_name + "_prices": [],
        crypto_name + "_market_cap": [],
        crypto_name + "_total_vol": []
    }
    js_line = 0
    for js_col in json:
        middle_js_line = len(json[js_col][js_line]) // 2
        for i in json[js_col]:
            if js_line <= 168:
                if json[js_col] == json["prices"]:
                    json_clean[crypto_name + "_timestamp"].extend(json[js_col][js_line][:middle_js_line])
                    json_clean[crypto_name + "_prices"].extend(json[js_col][js_line][middle_js_line:])
                elif json[js_col] == json["market_caps"]:
                    json_clean[crypto_name + "_market_cap"].extend(json[js_col][js_line][middle_js_line:])
                elif json[js_col] == json["total_volumes"]:
                    json_clean[crypto_name + "_total_vol"].extend(json[js_col][js_line][middle_js_line:])
                js_line += 1
        js_line = 0
    return json_clean

def producer_bitcoin():
    bitcoin = cg.get_coin_market_chart_by_id(
                id="bitcoin",
                tickers=False,
                vs_currency='usd',
                include_market_cap=True,
                days='7'
            )
    clean_bitcoin = clean_json(bitcoin, "BTC")
    return clean_bitcoin

def producer_ethereum():
    ethereum = cg.get_coin_market_chart_by_id(
                id="ethereum",
                vs_currency='usd',
                include_market_cap=True,
                days='7'
            )
    clean_ethereum = clean_json(ethereum, "ETH")
    return clean_ethereum

def producer_binancecoin():
    binancecoin = cg.get_coin_market_chart_by_id(
                    id="binancecoin",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_binancecoin = clean_json(binancecoin, "BNB")
    return clean_binancecoin

def producer_tether():
    tether = cg.get_coin_market_chart_by_id(
                    id="tether",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_tether = clean_json(tether, "USDT")
    return clean_tether

def producer_solana():
    solana = cg.get_coin_market_chart_by_id(
                    id="solana",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_solana = clean_json(solana, "SOL")
    return clean_solana

def producer_cardano():
    cardano = cg.get_coin_market_chart_by_id(
                    id="cardano",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_cardano = clean_json(cardano, "ADA")
    return clean_cardano

def producer_polkadot():
    polkadot = cg.get_coin_market_chart_by_id(
                    id="polkadot",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_polkadot = clean_json(polkadot, "DOT")
    return clean_polkadot

def producer_dogecoin():
    dogecoin = cg.get_coin_market_chart_by_id(
                    id="dogecoin",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_dogecoin = clean_json(dogecoin, "DOGE")
    return clean_dogecoin

def producer_litecoin():
    litecoin = cg.get_coin_market_chart_by_id(
                    id="litecoin",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    clean_litecoin = clean_json(litecoin, "LTC")
    return clean_litecoin

def call_crypto_api():
    json_full = {}
    bitcoin = producer_bitcoin()
    ethereum = producer_ethereum()
    binancecoin = producer_binancecoin()
    tether = producer_tether()
    solana = producer_solana()
    cardano = producer_cardano()
    polkadot = producer_polkadot()
    dogecoin = producer_dogecoin()
    litecoin = producer_litecoin()

    json_full.update(bitcoin)
    json_full.update(ethereum)
    json_full.update(binancecoin)
    json_full.update(tether)
    json_full.update(solana)
    json_full.update(cardano)
    json_full.update(polkadot)
    json_full.update(dogecoin)
    json_full.update(litecoin)
    return json_full

if __name__ == "__main__":
    
    try:
        producer = KafkaProducer(bootstrap_servers=BROKER)                                                                         
    except Exception as e:
        print(f"ERROR --> {e}")
        sys.exit(1)
    cg = CoinGeckoAPI()
    
    # while True:
    print("########## Send Data To Kafka: OK ##########")
    json_full = call_crypto_api()
    producer.send(TOPIC, json.dumps(json_full).encode('utf-8'))
    producer.flush()
    # sleep(5)