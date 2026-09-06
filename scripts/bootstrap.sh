#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

copy_env_if_missing() {
    local env_file="$1"
    local env_example="$2"

    if [[ ! -f "${env_file}" ]]; then
        cp "${env_example}" "${env_file}"
        echo "Created ${env_file} from ${env_example}"
    fi
}

copy_env_if_missing ".env" ".env.example"
copy_env_if_missing "disciple-registry/.env" "disciple-registry/.env.example"
copy_env_if_missing "resource-allocation/.env" "resource-allocation/.env.example"

echo "Starting local infrastructure..."
docker compose up -d

wait_for_healthy() {
    local container_name="$1"
    local timeout_seconds=120
    local elapsed=0
    local status=""

    while (( elapsed < timeout_seconds )); do
        status="$(docker inspect --format '{{.State.Health.Status}}' "${container_name}" 2>/dev/null || true)"

        if [[ "${status}" == "healthy" ]]; then
            return 0
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done

    docker compose ps
    echo "Container '${container_name}' did not become healthy within ${timeout_seconds} seconds." >&2
    exit 1
}

wait_for_healthy "cloud-stream-postgres"
wait_for_healthy "cloud-stream-kafka"

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --create \
    --if-not-exists \
    --topic disciple-registry.cultivation-advanced.v1 \
    --partitions 3 \
    --replication-factor 1

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --create \
    --if-not-exists \
    --topic resource-allocation.cultivation-advanced.dlq.v1 \
    --partitions 1 \
    --replication-factor 1

printf '\n'
echo "Local infrastructure is ready."
docker compose ps

printf '\n'
echo "Kafka topics:"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --list

printf '\n'
echo "Run the Go API and workers manually from their module directories."
