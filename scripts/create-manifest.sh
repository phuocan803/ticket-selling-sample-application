#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE_DIR="${ROOT_DIR}/src/app/templates"
OUT_FILE="${ROOT_DIR}/src/app/k8s-all.yaml"
PROFILE="${APP_PROFILE:-local}"
PROFILE_CONFIG="${TEMPLATE_DIR}/10-configmap-${PROFILE}.yaml"
LEGACY_CONFIG="${TEMPLATE_DIR}/10-configmap.yaml"
PROFILE_SECRET="${TEMPLATE_DIR}/12-secret-${PROFILE}.yaml"
LEGACY_SECRET="${TEMPLATE_DIR}/12-secret.yaml"

escape_sed() {
	printf '%s' "$1" | sed 's/[\\&|]/\\\\&/g'
}

if [[ ! -d "${TEMPLATE_DIR}" ]]; then
	echo "[ERROR] Template directory not found: ${TEMPLATE_DIR}" >&2
	exit 1
fi

echo "[INFO] Generating ${OUT_FILE} from ${TEMPLATE_DIR}"

if [[ "${PROFILE}" != "local" && "${PROFILE}" != "eks" ]]; then
	echo "[ERROR] APP_PROFILE must be one of: local, eks" >&2
	exit 1
fi

if [[ -f "${PROFILE_CONFIG}" ]]; then
  CONFIG_FILE="${PROFILE_CONFIG}"
elif [[ -f "${LEGACY_CONFIG}" ]]; then
  CONFIG_FILE="${LEGACY_CONFIG}"
else
  echo "[ERROR] Missing configmap template. Expected ${PROFILE_CONFIG} or ${LEGACY_CONFIG}" >&2
  exit 1
fi

if [[ -f "${PROFILE_SECRET}" ]]; then
	SECRET_FILE="${PROFILE_SECRET}"
elif [[ -f "${LEGACY_SECRET}" ]]; then
	SECRET_FILE="${LEGACY_SECRET}"
else
	echo "[ERROR] Missing secret template. Expected ${PROFILE_SECRET} or ${LEGACY_SECRET}" >&2
	exit 1
fi

echo "[INFO] Using config profile: ${PROFILE}"

AUTH_IMAGE="${AUTH_IMAGE:-}"
TICKETS_IMAGE="${TICKETS_IMAGE:-}"
ORDERS_IMAGE="${ORDERS_IMAGE:-}"
PAYMENTS_IMAGE="${PAYMENTS_IMAGE:-}"
EXPIRATION_IMAGE="${EXPIRATION_IMAGE:-}"
CLIENT_IMAGE="${CLIENT_IMAGE:-}"

if [[ "${PROFILE}" == "eks" ]]; then
	required_envs=(
		AWS_REGION
		SQS_ORDER_EVENTS_QUEUE_URL
		SQS_EXPIRATION_QUEUE_URL
		SQS_EXPIRATION_EVENTS_QUEUE_URL
		SQS_PAYMENT_EVENTS_QUEUE_URL
		ORDERS_IRSA_ROLE_ARN
		PAYMENTS_IRSA_ROLE_ARN
		EXPIRATION_IRSA_ROLE_ARN
	)

	for env_name in "${required_envs[@]}"; do
		if [[ -z "${!env_name:-}" ]]; then
			echo "[ERROR] ${env_name} is required when APP_PROFILE=eks" >&2
			exit 1
		fi
	done

	ECR_REGISTRY="${ECR_REGISTRY:-}"
	IMAGE_TAG="${IMAGE_TAG:-latest}"
	if [[ -z "${ECR_REGISTRY}" ]]; then
		echo "[ERROR] ECR_REGISTRY is required when APP_PROFILE=eks" >&2
		exit 1
	fi

	AUTH_IMAGE="${AUTH_IMAGE:-${ECR_REGISTRY}/ticket-selling-auth:${IMAGE_TAG}}"
	TICKETS_IMAGE="${TICKETS_IMAGE:-${ECR_REGISTRY}/ticket-selling-tickets:${IMAGE_TAG}}"
	ORDERS_IMAGE="${ORDERS_IMAGE:-${ECR_REGISTRY}/ticket-selling-orders:${IMAGE_TAG}}"
	PAYMENTS_IMAGE="${PAYMENTS_IMAGE:-${ECR_REGISTRY}/ticket-selling-payments:${IMAGE_TAG}}"
	EXPIRATION_IMAGE="${EXPIRATION_IMAGE:-${ECR_REGISTRY}/ticket-selling-expiration:${IMAGE_TAG}}"
	CLIENT_IMAGE="${CLIENT_IMAGE:-${ECR_REGISTRY}/ticket-selling-client:${IMAGE_TAG}}"
else
	AUTH_IMAGE="${AUTH_IMAGE:-ticket-selling/auth:local}"
	TICKETS_IMAGE="${TICKETS_IMAGE:-ticket-selling/tickets:local}"
	ORDERS_IMAGE="${ORDERS_IMAGE:-ticket-selling/orders:local}"
	PAYMENTS_IMAGE="${PAYMENTS_IMAGE:-ticket-selling/payments:local}"
	EXPIRATION_IMAGE="${EXPIRATION_IMAGE:-ticket-selling/expiration:local}"
	CLIENT_IMAGE="${CLIENT_IMAGE:-ticket-selling/client:local}"
fi

bundle_files=(
	"${TEMPLATE_DIR}/00-namespace.yaml"
	"${CONFIG_FILE}"
	"${SECRET_FILE}"
	"${TEMPLATE_DIR}/15-serviceaccounts.yaml"
	"${TEMPLATE_DIR}/20-deployments.yaml"
	"${TEMPLATE_DIR}/30-services.yaml"
	"${TEMPLATE_DIR}/40-ingress.yaml"
)

{
	for idx in "${!bundle_files[@]}"; do
		cat "${bundle_files[$idx]}"
		if [[ "$idx" -lt "$((${#bundle_files[@]} - 1))" ]]; then
			echo
			echo "---"
		fi
	done
} > "${OUT_FILE}"

if [[ "${PROFILE}" == "eks" ]]; then
	sed -i \
		-e "s|__AWS_REGION__|$(escape_sed "${AWS_REGION}")|g" \
		-e "s|__SQS_ORDER_EVENTS_QUEUE_URL__|$(escape_sed "${SQS_ORDER_EVENTS_QUEUE_URL}")|g" \
		-e "s|__SQS_EXPIRATION_QUEUE_URL__|$(escape_sed "${SQS_EXPIRATION_QUEUE_URL}")|g" \
		-e "s|__SQS_EXPIRATION_EVENTS_QUEUE_URL__|$(escape_sed "${SQS_EXPIRATION_EVENTS_QUEUE_URL}")|g" \
		-e "s|__SQS_PAYMENT_EVENTS_QUEUE_URL__|$(escape_sed "${SQS_PAYMENT_EVENTS_QUEUE_URL}")|g" \
		-e "s|__ORDERS_IRSA_ROLE_ARN__|$(escape_sed "${ORDERS_IRSA_ROLE_ARN}")|g" \
		-e "s|__PAYMENTS_IRSA_ROLE_ARN__|$(escape_sed "${PAYMENTS_IRSA_ROLE_ARN}")|g" \
		-e "s|__EXPIRATION_IRSA_ROLE_ARN__|$(escape_sed "${EXPIRATION_IRSA_ROLE_ARN}")|g" \
		"${OUT_FILE}"
fi

sed -i \
	-e "s|__AUTH_IMAGE__|$(escape_sed "${AUTH_IMAGE}")|g" \
	-e "s|__TICKETS_IMAGE__|$(escape_sed "${TICKETS_IMAGE}")|g" \
	-e "s|__ORDERS_IMAGE__|$(escape_sed "${ORDERS_IMAGE}")|g" \
	-e "s|__PAYMENTS_IMAGE__|$(escape_sed "${PAYMENTS_IMAGE}")|g" \
	-e "s|__EXPIRATION_IMAGE__|$(escape_sed "${EXPIRATION_IMAGE}")|g" \
	-e "s|__CLIENT_IMAGE__|$(escape_sed "${CLIENT_IMAGE}")|g" \
	"${OUT_FILE}"

echo "[DONE] Manifest bundle created: ${OUT_FILE}"
