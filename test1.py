import json
import time
import random

from kafka import KafkaProducer

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

data = {'RType': 'B', 'TradingDate': '08/03/2024', 'Time': '14:45:02', 'Symbol': 'VPB', 'Open': 19000.0, 'High': 19000.0, 'Low': 19000.0, 'Close': 19000.0, 'Volume': 1124000.0, 'Value': 0.0}

for i in range(50):
    kafka_message = producer.send('hose', data)

    # Chờ phản hồi
    record_metadata = kafka_message.get()

    # Kiểm tra phản hồi
    if record_metadata.topic == 'hose':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")

    # time.sleep(random.random())
