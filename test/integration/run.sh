#!/bin/sh

set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compose_file="$root/test/integration/compose.yaml"

cleanup() {
	status=$?
	docker compose --file "$compose_file" down --volumes
	exit "$status"
}
trap cleanup EXIT

docker compose --file "$compose_file" up --detach --wait

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:19092 \
	--create \
	--if-not-exists \
	--topic test-ready \
	--partitions 1 \
	--replication-factor 1

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:19092 \
	--create \
	--if-not-exists \
	--topic concurrency-test-ready \
	--partitions 1 \
	--replication-factor 1

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:19092 \
	--create \
	--if-not-exists \
	--topic retry-test-ready \
	--partitions 1 \
	--replication-factor 1

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:19092 \
	--create \
	--if-not-exists \
	--topic retry-test-retry-0s \
	--partitions 4 \
	--replication-factor 1 \
	--config message.timestamp.type=LogAppendTime

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:19092 \
	--create \
	--if-not-exists \
	--topic dlq-test-ready \
	--partitions 1 \
	--replication-factor 1

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server localhost:19092 \
	--create \
	--if-not-exists \
	--topic dlq-test-dlq \
	--partitions 1 \
	--replication-factor 1

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server localhost:19092 \
	--alter \
	--entity-type groups \
	--entity-name kq.test.workers \
	--add-config share.auto.offset.reset=earliest

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server localhost:19092 \
	--alter \
	--entity-type groups \
	--entity-name kq.retry-test.workers \
	--add-config share.auto.offset.reset=earliest

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server localhost:19092 \
	--alter \
	--entity-type groups \
	--entity-name kq.concurrency-test.workers \
	--add-config share.auto.offset.reset=earliest

docker compose --file "$compose_file" exec -T kafka \
	/opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server localhost:19092 \
	--alter \
	--entity-type groups \
	--entity-name kq.dlq-test.workers \
	--add-config share.auto.offset.reset=earliest

KQ_TEST_BROKERS=localhost:19092 \
	go test -tags=integration -count=1 -timeout=2m ./test/integration
