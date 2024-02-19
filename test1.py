from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('demo',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='latest',
                         value_deserializer=lambda x: json.loads(x.decode('utf8')))

for e in consumer:
    print(e.value)
