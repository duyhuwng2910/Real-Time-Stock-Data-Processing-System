from kafka import KafkaConsumer, KafkaAdminClient
import json


bootstrap_servers = ['localhost:29093', 'localhost:29094']

consumer = KafkaConsumer('demo',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='latest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

llist = []

try:
    # Bắt đầu consumer
    for message in consumer:
        llist.append(message.value)
        # Xử lý dữ liệu
        print(message.value)
except (TypeError, KeyError):
    print("Không thể xác định leader partition. Kiểm tra kết nối, tên topic hoặc cấu hình Kafka.")

print(len(llist))
