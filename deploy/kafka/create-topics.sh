#!/bin/bash
# Creates the Kafka topics from ADR-0002. Idempotent: safe to run on every `compose up`.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
RF="${KAFKA_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}" # 7 days
KT=/opt/kafka/bin/kafka-topics.sh

create() { # name partitions
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor "$RF" \
    --config retention.ms="$RETENTION_MS"
}

for channel in sms email push; do
  create "notifications.${channel}.high" 6
  create "notifications.${channel}.normal" 6
  create "notifications.${channel}.low" 6
done
create notifications.dlq 3
create notification.events 6

"$KT" --bootstrap-server "$BOOTSTRAP" --list
