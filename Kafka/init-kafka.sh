### Kafka
# Truy cập vào cửa sổ dòng lệnh của Kafka container
docker exec -it kafka-controller-1 bash

# Tạo kafka topic
# Ở đây, tạo một topic với số replication factor là 2 và partitions là 2
kafka-topics --create --topic hose --bootstrap-server kafka-controller-1:9092 --replication-factor 2 --partitions 2

# Kiểm tra danh sách các topic có trong hệ thống
kafka-topics --list --bootstrap-server kafka-controller-1:9092

# Kiểm tra thông tin về topic đã tạo
kafka-topics --describe --topic hose --bootstrap-server kafka-controller-1:9092

# Kiểm tra bản tin được gửi lên topic
kafka-console-consumer --topic hose --bootstrap-server kafka-broker-1:9093