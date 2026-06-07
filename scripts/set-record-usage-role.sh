#!/usr/bin/env bash
# Register the router's record-usage signing key as SettlementEscrow.recordUsageRole.
#
# The registry owner (ProviderRegistry.owner) must submit setRecordUsage(address).
# On local Anvil that is usually account 0 — configure settlement.registry_owner_private_key
# in config.toml (same key used at deploy time).
#
# Usage:
#   ./scripts/set-record-usage-role.sh [config.toml]
#   ./scripts/set-record-usage-role.sh config.toml --check-only
#   ./scripts/set-record-usage-role.sh config.toml --role-address 0x...
#   REGISTRY_OWNER_PRIVATE_KEY=0x... ./scripts/set-record-usage-role.sh
#
# Requires: cast, python3, jq
# Optional: sync escrow/registry from solo deploy:
#   --from-deploy-json ../sparkl-solo/contracts/deployments/local.json
#
# If recordUsageRole() reverts, the escrow bytecode is too old — redeploy contracts first:
#   cd ../sparkl-solo && ./scripts/deploy-local-sync-env.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/onchain-common.sh
source "${ROOT}/scripts/lib/onchain-common.sh"

CONFIG="${ROOT}/config.toml"
CHECK_ONLY=0
ROLE_OVERRIDE=""
FROM_DEPLOY_JSON=""
REGISTRY_OWNER_PK="${REGISTRY_OWNER_PRIVATE_KEY:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=1 ;;
    --role-address) ROLE_OVERRIDE="$2"; shift ;;
    --from-deploy-json) FROM_DEPLOY_JSON="$2"; shift ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      CONFIG="$1"
      ;;
  esac
  shift
done

if [[ ! "${CONFIG}" = /* ]]; then
  CONFIG="${ROOT}/${CONFIG}"
fi

onchain_require_cmd cast
onchain_require_cmd python3
onchain_require_cmd jq

RPC_URL="$(onchain_toml_get "${CONFIG}" rpc_url || true)"
ESCROW="$(onchain_toml_get "${CONFIG}" escrow_contract || true)"
REGISTRY="$(onchain_toml_get "${CONFIG}" registry_contract || true)"
DATA_DIR_CFG="$(onchain_toml_get "${CONFIG}" data_dir 2>/dev/null || echo data)"
if [[ -z "${REGISTRY_OWNER_PK}" ]]; then
  REGISTRY_OWNER_PK="$(onchain_toml_get "${CONFIG}" registry_owner_private_key 2>/dev/null || true)"
fi

if [[ -n "${FROM_DEPLOY_JSON}" ]]; then
  if [[ ! -f "${FROM_DEPLOY_JSON}" ]]; then
    echo "Deploy JSON not found: ${FROM_DEPLOY_JSON}" >&2
    exit 1
  fi
  ESCROW="$(jq -r '.settlementEscrow' "${FROM_DEPLOY_JSON}")"
  REGISTRY="$(jq -r '.providerRegistry' "${FROM_DEPLOY_JSON}")"
  RPC_URL="${RPC_URL:-http://127.0.0.1:8545}"
  echo "Using deploy JSON: escrow=${ESCROW} registry=${REGISTRY}"
fi

if [[ -z "${RPC_URL}" || -z "${ESCROW}" || -z "${REGISTRY}" ]]; then
  echo "config must define [chain] rpc_url, escrow_contract, registry_contract" >&2
  exit 1
fi

DATA_DIR="$(onchain_resolve_data_dir "${CONFIG}" "${DATA_DIR_CFG}")"
KEY_FILE="${DATA_DIR}/record-usage-key.json"

if [[ -n "${ROLE_OVERRIDE}" ]]; then
  ROLE_ADDRESS="${ROLE_OVERRIDE}"
elif [[ -f "${KEY_FILE}" ]]; then
  ROLE_ADDRESS="$(jq -r '.address' "${KEY_FILE}")"
else
  echo "No record-usage key at ${KEY_FILE}" >&2
  echo "Start sparkl-router once (it generates data/record-usage-key.json) or pass --role-address." >&2
  exit 1
fi

echo "RPC:              ${RPC_URL}"
echo "SettlementEscrow: ${ESCROW}"
echo "ProviderRegistry: ${REGISTRY}"
echo "recordUsageRole:  ${ROLE_ADDRESS}"

onchain_wait_for_rpc "${RPC_URL}"

if ! onchain_escrow_has_record_usage_role "${ESCROW}" "${RPC_URL}"; then
  echo "" >&2
  echo "ERROR: SettlementEscrow at ${ESCROW} does not support recordUsageRole()." >&2
  echo "  The contract on this chain is an older build (getter reverts)." >&2
  echo "" >&2
  echo "Redeploy local contracts, then re-run this script:" >&2
  echo "  cd \"${ROOT}/../sparkl-solo\" && ./scripts/deploy-local-sync-env.sh" >&2
  echo "  # updates sparkl-router config.toml escrow/registry addresses" >&2
  echo "  cd \"${ROOT}\" && ./scripts/set-record-usage-role.sh \"${CONFIG}\"" >&2
  exit 2
fi

CURRENT_ROLE="$(cast call "${ESCROW}" "recordUsageRole()(address)" --rpc-url "${RPC_URL}")"
echo "On-chain role:    ${CURRENT_ROLE}"

if [[ "${CURRENT_ROLE,,}" == "${ROLE_ADDRESS,,}" ]]; then
  echo "recordUsageRole already matches the router key."
  if [[ -z "${REGISTRY_OWNER_PK}" ]]; then
    REGISTRY_OWNER_PK="$(onchain_toml_get "${CONFIG}" registry_owner_private_key 2>/dev/null || true)"
  fi
  ROLE_BALANCE="$(cast balance "${ROLE_ADDRESS}" --rpc-url "${RPC_URL}")"
  if [[ "${ROLE_BALANCE}" == "0" && -n "${REGISTRY_OWNER_PK}" ]]; then
    echo "Funding recordUsageRole with 0.1 ETH for transaction gas..."
    cast send "${ROLE_ADDRESS}" --value 0.1ether \
      --rpc-url "${RPC_URL}" \
      --private-key "${REGISTRY_OWNER_PK}" \
      --quiet
    echo "  new balance: $(cast balance "${ROLE_ADDRESS}" --rpc-url "${RPC_URL}") wei"
  fi
  exit 0
fi

if [[ "${CHECK_ONLY}" -eq 1 ]]; then
  echo "Check only: role would be updated from ${CURRENT_ROLE} -> ${ROLE_ADDRESS}"
  exit 0
fi

if [[ -z "${REGISTRY_OWNER_PK}" ]]; then
  echo "" >&2
  echo "ERROR: registry owner private key required to call setRecordUsage." >&2
  echo "  Set settlement.registry_owner_private_key in ${CONFIG}" >&2
  echo "  or export REGISTRY_OWNER_PRIVATE_KEY (must be ProviderRegistry.owner)." >&2
  exit 1
fi

OWNER="$(cast call "${REGISTRY}" "owner()(address)" --rpc-url "${RPC_URL}")"
SIGNER="$(cast wallet address --private-key "${REGISTRY_OWNER_PK}")"
echo "Registry owner:   ${OWNER}"
echo "Tx signer:        ${SIGNER}"

if [[ "${OWNER,,}" != "${SIGNER,,}" ]]; then
  echo "ERROR: signer ${SIGNER} is not registry owner ${OWNER}" >&2
  exit 1
fi

echo "Submitting setRecordUsage(${ROLE_ADDRESS})..."
cast send "${ESCROW}" "setRecordUsage(address)" "${ROLE_ADDRESS}" \
  --rpc-url "${RPC_URL}" \
  --private-key "${REGISTRY_OWNER_PK}"

NEW_ROLE="$(cast call "${ESCROW}" "recordUsageRole()(address)" --rpc-url "${RPC_URL}")"
echo "Updated on-chain recordUsageRole: ${NEW_ROLE}"

ROLE_BALANCE="$(cast balance "${ROLE_ADDRESS}" --rpc-url "${RPC_URL}")"
if [[ "${ROLE_BALANCE}" == "0" ]]; then
  echo "Funding recordUsageRole with 0.1 ETH for transaction gas..."
  cast send "${ROLE_ADDRESS}" --value 0.1ether \
    --rpc-url "${RPC_URL}" \
    --private-key "${REGISTRY_OWNER_PK}" \
    --quiet
  echo "  new balance: $(cast balance "${ROLE_ADDRESS}" --rpc-url "${RPC_URL}") wei"
else
  echo "recordUsageRole balance: ${ROLE_BALANCE} wei"
fi

echo "Restart sparkl-router to enable the usage batcher."
