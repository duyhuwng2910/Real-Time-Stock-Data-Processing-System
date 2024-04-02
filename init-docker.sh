docker network create graduation-thesis

# start kafka
docker compose -f Kafka/docker-compose.yaml --profile all up -d

# Start other containers
docker compose up -d

# Start monitoring containers
docker compose -f Monitor/docker-compose.yaml up -d