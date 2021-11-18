from pycoingecko import CoinGeckoAPI
from kafka import KafkaProducer
import numpy as np
import json
import time
import sys

BROKER = 'localhost:9092'
TOPIC = 'crypto1'
list_crypto = ['bitcoin', 'ethereum', 'binancecoin', 'tether', 'solana', 'cardano', 'polkadot', 'dogecoin', 'litecoin']

def producer_bitcoin():
    bitcoin = cg.get_coin_market_chart_by_id(
                id="bitcoin",
                tickers=True,
                vs_currency='usd',
                include_market_cap=True,
                days='7'
            )
    bitcoin["tag"] = "BTC"
    return bitcoin

def producer_ethereum():
    ethereum = cg.get_coin_market_chart_by_id(
                id="ethereum",
                vs_currency='usd',
                include_market_cap=True,
                days='7'
            )
    ethereum["tag"] = "ETH"
    return ethereum

def producer_binancecoin():
    binancecoin = cg.get_coin_market_chart_by_id(
                    id="binancecoin",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    binancecoin["tag"] = "BNB"
    return binancecoin

def producer_tether():
    tether = cg.get_coin_market_chart_by_id(
                    id="tether",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    tether["tag"] = "USDT"
    return tether

def producer_solana():
    solana = cg.get_coin_market_chart_by_id(
                    id="solana",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    solana["tag"] = "SOL"
    return solana

def producer_cardano():
    cardano = cg.get_coin_market_chart_by_id(
                    id="cardano",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    cardano["tag"] = "ADA"
    return cardano

def producer_polkadot():
    polkadot = cg.get_coin_market_chart_by_id(
                    id="polkadot",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    polkadot["tag"] = "DOT"
    return polkadot

def producer_dogecoin():
    dogecoin = cg.get_coin_market_chart_by_id(
                    id="dogecoin",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    dogecoin["tag"] = "DOGE"
    return dogecoin

def producer_litecoin():
    litecoin = cg.get_coin_market_chart_by_id(
                    id="litecoin",
                    vs_currency='usd',
                    include_market_cap=True,
                    days='7'
                )
    litecoin["tag"] = "LTC"
    return litecoin

if __name__ == "__main__":
    
    try:
        producer = KafkaProducer(bootstrap_servers=BROKER)                                                                         
    except Exception as e:
        print(f"ERROR --> {e}")
        sys.exit(1)
    
    cg = CoinGeckoAPI()
    print("Send data to kafka: OK")
    bitcoin = producer_bitcoin()
    ethereum = producer_ethereum()
    binancecoin = producer_binancecoin()
    tether = producer_tether()
    solana = producer_solana()
    cardano = producer_cardano()
    polkadot = producer_polkadot()
    dogecoin = producer_dogecoin()
    litecoin = producer_litecoin()

    # while True:
    producer.send(TOPIC, json.dumps(bitcoin).encode('utf-8'))
    producer.send(TOPIC, json.dumps(ethereum).encode('utf-8'))
    producer.send(TOPIC, json.dumps(binancecoin).encode('utf-8'))
    producer.send(TOPIC, json.dumps(tether).encode('utf-8'))
    producer.send(TOPIC, json.dumps(solana).encode('utf-8'))
    producer.send(TOPIC, json.dumps(cardano).encode('utf-8'))
    producer.send(TOPIC, json.dumps(polkadot).encode('utf-8'))
    producer.send(TOPIC, json.dumps(dogecoin).encode('utf-8'))
    producer.send(TOPIC, json.dumps(litecoin).encode('utf-8'))
    producer.flush()
    # sleep(5)