from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('demo',
                         bootstrap_servers='localhost:9094',
                         auto_offset_reset='latest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

llist = []

for e in consumer:
    print(e.value)
    llist.append(e.value)

print(len(llist))
