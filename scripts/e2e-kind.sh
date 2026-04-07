#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-ticket-selling}"
NAMESPACE="${NAMESPACE:-ticket-selling}"

for tool in docker kubectl kind; do
	if ! command -v "${tool}" >/dev/null 2>&1; then
		echo "[ERROR] Missing required tool: ${tool}" >&2
		exit 1
	fi
done

echo "[INFO] Recreating kind cluster ${CLUSTER_NAME}"
kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
kind create cluster --name "${CLUSTER_NAME}"

echo "[INFO] Installing ingress-nginx"
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.1/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=300s

echo "[INFO] Loading local images into kind"
for svc in auth tickets orders payments expiration client; do
	docker build -t "ticket-selling/${svc}:local" "${ROOT_DIR}/src/${svc}"
	kind load docker-image --name "${CLUSTER_NAME}" "ticket-selling/${svc}:local"
done

echo "[INFO] Deploying manifests"
"${SCRIPT_DIR}/kubernetes-dist.sh"

echo "[INFO] Smoke checks"
kubectl -n "${NAMESPACE}" get pods
kubectl -n "${NAMESPACE}" get svc
kubectl -n "${NAMESPACE}" get ingress

echo "[DONE] kind e2e completed"
