.PHONY: install up down produce stream test consume-dlq consume-alerts clean

KAFKA_CONSUMER = docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092

install:
	python -m venv .venv && .venv/bin/pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down -v

produce:
	python -m producer.clickstream_producer --rate 10

stream:
	python -m streaming.stream_processor

test:
	python -m pytest -v

consume-dlq:
	$(KAFKA_CONSUMER) --topic clickstream-dlq --from-beginning

consume-alerts:
	$(KAFKA_CONSUMER) --topic clickstream-alerts --from-beginning

clean:
	rm -rf output checkpoints spark-warehouse
