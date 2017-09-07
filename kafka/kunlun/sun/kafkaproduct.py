import time
from pykafka import KafkaClient

client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics['test']
with topic.get_sync_producer() as producer:
    for i in range(400):
        producer.produce("abc:"+i)
        time.sleep(2)