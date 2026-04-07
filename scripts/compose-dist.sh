#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/src/app/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1; then
	echo "[ERROR] docker is required" >&2
	exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
	echo "[ERROR] docker compose is required" >&2
	exit 1
fi

echo "[INFO] Building service images"
for svc in auth tickets orders payments expiration client; do
	docker build -t "ticket-selling/${svc}:local" "${ROOT_DIR}/src/${svc}"
done

echo "[INFO] Starting local stack with ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" up -d

echo "[DONE] Compose stack started"
