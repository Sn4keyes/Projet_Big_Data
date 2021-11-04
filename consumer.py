from kafka import KafkaConsumer
import json

c = KafkaConsumer('test5', bootstrap_servers=['kafka:9093'], api_version=(2,6,0))

def process_msg(msg):
    print(msg.offset)
    print(json.loads(msg.value))

for msg in c:
    process_msg(msg)