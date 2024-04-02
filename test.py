import json
import random
import time

from kafka import KafkaProducer

'''
    Kafka configuration
'''
bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

data = {'RTYpe': 'B', 'TradingDate': '22/03/2024', 'Time': '14:44:59', 'Symbol': 'VIC', 'Open': 50000.0, 'High': 50000.0, 'Low': 50000.0, 'Close': 50000.0, 'Volume': 500000.0, 'Value': 0.0}


def main():
    for i in range(50):
        for j in range(200):
            producer.send('hose', data)

        time.sleep(random.random())


main()
