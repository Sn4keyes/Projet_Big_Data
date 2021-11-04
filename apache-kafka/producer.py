import numpy as np
from kafka import KafkaProducer
import json
import time

p = KafkaProducer(bootstrap_servers=['localhost:9092'])
i = 0
while True:
    v = np.random.uniform(0, 2, 100)
    data1 = v.tolist()
    data = {'id': i,
            'type': 'uni',
            'data': data1}
    p.send('test6', json.dumps(data).encode('utf-8'))
    p.flush()
    i += 1
    time.sleep(1)