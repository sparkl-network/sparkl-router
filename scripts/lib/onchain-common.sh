# Shared helpers for sparkl-router on-chain scripts (sourced, not executed).

onchain_require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

# Read a top-level TOML string or integer field from a router config file.
# Usage: onchain_toml_get <config.toml> <key>
onchain_toml_get() {
  local file="$1" key="$2"
  python3 - "$file" "$key" <<'PY'
import pathlib, re, sys

path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
text = path.read_text()
match = re.search(rf"^{re.escape(key)}\s*=\s*(.+)$", text, re.MULTILINE)
if not match:
    sys.exit(1)
raw = match.group(1).strip()
if raw.startswith('"') and raw.endswith('"'):
    print(raw[1:-1])
elif raw.startswith("'") and raw.endswith("'"):
    print(raw[1:-1])
else:
    print(raw.split("#", 1)[0].strip())
PY
}

# Resolve router data_dir relative to config file (matches Rust resolve_data_dir).
onchain_resolve_data_dir() {
  local config_path="$1" configured="$2"
  local trimmed="${configured#"${configured%%[![:space:]]*}"}"
  trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
  if [[ -n "${trimmed}" ]]; then
    if [[ "${trimmed}" = /* ]]; then
      printf '%s' "${trimmed}"
    else
      printf '%s/%s' "$(dirname "${config_path}")" "${trimmed}"
    fi
    return 0
  fi
  printf '%s/data' "$(dirname "${config_path}")"
}

onchain_wait_for_rpc() {
  local rpc="$1" tries="${2:-40}"
  while (( tries > 0 )); do
    if cast chain-id --rpc-url "${rpc}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
    tries=$((tries - 1))
  done
  echo "RPC not ready at ${rpc}" >&2
  return 1
}

# Returns 0 when recordUsageRole() succeeds, 1 when the getter reverts (stale escrow bytecode).
onchain_escrow_has_record_usage_role() {
  local escrow="$1" rpc="$2"
  cast call "${escrow}" "recordUsageRole()(address)" --rpc-url "${rpc}" >/dev/null 2>&1
}
