#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${NAMESPACE:-ticket-selling}"

if ! command -v kubectl >/dev/null 2>&1; then
	echo "[ERROR] kubectl is required" >&2
	exit 1
fi

"${SCRIPT_DIR}/create-manifest.sh"

echo "[INFO] Applying manifests to namespace ${NAMESPACE}"
kubectl apply -f "${ROOT_DIR}/src/app/k8s-all.yaml"

echo "[INFO] Waiting for deployments"
for svc in auth tickets orders payments expiration client; do
	kubectl -n "${NAMESPACE}" rollout status "deployment/${svc}" --timeout=180s
done

echo "[DONE] Kubernetes distribution applied successfully"
