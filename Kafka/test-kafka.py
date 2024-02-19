import time

from kafka import KafkaProducer
import json


producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for i in range(100):
    message = {'a': i}
    print(message)

    kafka_message = producer.send('demo', message)

    # Chờ phản hồi
    record_metadata = kafka_message.get()

    # Kiểm tra phản hồi
    if record_metadata.topic == 'demo':
        print("Dữ liệu đã được gửi thành công lên topic")
    else:
        print("Lỗi: Dữ liệu không được gửi")

    time.sleep(1)

producer.flush()

producer.close()
