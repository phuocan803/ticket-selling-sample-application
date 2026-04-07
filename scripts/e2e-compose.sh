#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

for tool in docker curl; do
	if ! command -v "${tool}" >/dev/null 2>&1; then
		echo "[ERROR] Missing required tool: ${tool}" >&2
		exit 1
	fi
done

"${SCRIPT_DIR}/compose-dist.sh"

echo "[INFO] Waiting for services to boot"
sleep 5

declare -A svc_ports=(
	[auth]=3001
	[tickets]=3002
	[orders]=3003
	[payments]=3004
	[expiration]=3005
	[client]=3000
)

for svc in auth tickets orders payments expiration client; do
	echo "[INFO] Checking ${svc}"
	curl -fsS "http://localhost:${svc_ports[$svc]}/healthz" >/dev/null
done

echo "[DONE] compose e2e smoke checks passed"
