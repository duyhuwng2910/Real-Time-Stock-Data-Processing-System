import time

from kafka import KafkaProducer
import json


# Địa chỉ bootstrap server (thay đổi nếu cần)
bootstrap_servers = ['localhost:29093', 'localhost:29094']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for i in range(100):
    message = {'a': i}
    print(message)

    kafka_message = producer.send('demo', message)

    # Chờ phản hồi
    record_metadata = kafka_message.get(5)

    # Kiểm tra phản hồi
    if record_metadata.topic == 'demo':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")

    time.sleep(1)

producer.flush()

producer.close()
