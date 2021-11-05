import numpy as np
from kafka import KafkaProducer
import json
import time
import requests

d = requests.get("https://data.messari.io/api/v1/assets?fields=id,slug,symbol,metrics/market_data/price_usd").json()
p = KafkaProducer(bootstrap_servers='localhost:9092')
p.send('test7', json.dumps(d).encode('utf-8'))
p.flush()
print(d)