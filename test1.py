import json
import time

from kafka import KafkaProducer

bootstrap_servers = ['localhost:29093', 'localhost:29094', 'localhost:29095']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

data = {"RType":"B","TradingDate":"05/03/2024","Time":"14:45:00","Symbol":"HPG","Open":31150.0,"High":31150.0,"Low":31150.0,"Close":31150.0,"Volume":1473600.0,"Value":0.0}

for i in range(100):
    kafka_message = producer.send('demo', data)

    # Chờ phản hồi
    record_metadata = kafka_message.get()

    # Kiểm tra phản hồi
    if record_metadata.topic == 'demo':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")

    time.sleep(2)