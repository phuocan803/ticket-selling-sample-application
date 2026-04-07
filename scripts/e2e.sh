#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-kind}"

case "${MODE}" in
	kind)
		"${SCRIPT_DIR}/e2e-kind.sh"
		;;
	compose)
		"${SCRIPT_DIR}/e2e-compose.sh"
		;;
	*)
		echo "Usage: $0 [kind|compose]" >&2
		exit 1
		;;
esac
