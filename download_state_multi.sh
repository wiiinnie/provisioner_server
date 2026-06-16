#!/bin/bash
set -e

#############################################
# Multi-Instance Dusk State Downloader
#
# Downloads a published state snapshot from nodes.dusk.network and
# replaces the chain.db + state in /opt/dusk{N}/rusk/.
#
# Based on the official upstream download_state.sh with multi-instance
# support added.
#############################################

# Defaults
INSTANCE=1
NETWORK=""
LIST_STATES=0
STATE_NUMBER=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

usage() {
  cat << EOF
Usage: $0 [OPTIONS] [state_number]

OPTIONS:
  --instance N           Instance to update (default: 1)
                         Targets /opt/dusk{N} and service rusk-{N}
  --network NETWORK      Force network (mainnet|testnet)
                         (auto-detected from rusk.toml if omitted)
  --list                 List available state snapshots and exit
  -h, --help             Show this help

POSITIONAL:
  state_number           Specific state to download (default: latest)

EXAMPLES:
  # Download latest state for instance 1 (auto-detect network)
  sudo bash $0

  # Download latest state for instance 3 on testnet
  sudo bash $0 --instance 3 --network testnet

  # List available state snapshots for mainnet
  sudo bash $0 --network mainnet --list

  # Download specific state number for instance 2
  sudo bash $0 --instance 2 4500000

NOTES:
  - This will STOP the rusk-{N} service before replacing state.
  - The script removes /opt/dusk{N}/rusk/state and /opt/dusk{N}/rusk/chain.db
    before extracting the new snapshot.
  - The service is NOT auto-started; restart it manually after verification:
      sudo systemctl start rusk-{N}
EOF
}

#############################################
# Argument parsing
#############################################
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --instance)
      if [[ -z "$2" || "$2" == --* ]]; then
        echo -e "${RED}Error: --instance requires a value.${NC}"
        usage; exit 1
      fi
      INSTANCE="$2"
      shift 2
      ;;
    --network)
      if [[ -z "$2" || "$2" == --* ]]; then
        echo -e "${RED}Error: --network requires a value.${NC}"
        usage; exit 1
      fi
      NETWORK="$2"
      shift 2
      ;;
    --list)
      LIST_STATES=1
      shift
      ;;
    -h|--help)
      usage; exit 0
      ;;
    *)
      if [[ -n "$STATE_NUMBER" ]]; then
        echo -e "${RED}Error: Unexpected argument '$1'.${NC}"
        usage; exit 1
      fi
      STATE_NUMBER="$1"
      shift
      ;;
  esac
done

#############################################
# Validation
#############################################
if ! [[ "$INSTANCE" =~ ^[0-9]+$ ]] || [ "$INSTANCE" -lt 1 ]; then
  echo -e "${RED}Error: --instance must be a positive integer (got: '$INSTANCE').${NC}"
  exit 1
fi

if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Error: This script must be run as root (use sudo).${NC}"
  exit 1
fi

#############################################
# Computed paths
#############################################
DUSK_ROOT="/opt/dusk${INSTANCE}"
SERVICE_NAME="rusk-${INSTANCE}"
RUSK_CONFIG_FILE="${DUSK_ROOT}/conf/rusk.toml"

if [ ! -d "$DUSK_ROOT" ]; then
  echo -e "${RED}Error: Instance ${INSTANCE} not found — directory ${DUSK_ROOT} does not exist.${NC}"
  echo "Run the node installer first, or check the --instance value."
  exit 1
fi

#############################################
# Network detection
#############################################
detect_network() {
  if [ -f "$RUSK_CONFIG_FILE" ]; then
    case "$(grep -E "^kadcast_id" "$RUSK_CONFIG_FILE" | head -n 1 | awk -F= '{print $2}' | tr -d "[:space:]")" in
      0x1) printf '%s\n' "mainnet"; return 0 ;;
      0x2) printf '%s\n' "testnet"; return 0 ;;
    esac
  fi
  return 1
}

if [[ -z "$NETWORK" ]]; then
  if ! NETWORK=$(detect_network); then
    echo -e "${RED}Error: Failed to detect network from $RUSK_CONFIG_FILE.${NC}"
    echo "Use --network mainnet|testnet explicitly."
    exit 1
  fi
fi

case "$NETWORK" in
  mainnet)
    STATE_BASE_URL="https://nodes.dusk.network/state"
    ;;
  testnet)
    STATE_BASE_URL="https://testnet.nodes.dusk.network/state"
    ;;
  *)
    echo -e "${RED}Error: Unsupported network '$NETWORK' (must be mainnet or testnet).${NC}"
    exit 1
    ;;
esac

STATE_LIST_URL="$STATE_BASE_URL/list"

#############################################
# Helpers
#############################################
display_warning() {
  echo ""
  echo -e "${GREEN}========================================${NC}"
  echo -e "Instance:         ${YELLOW}${INSTANCE}${NC}"
  echo -e "Service:          ${YELLOW}${SERVICE_NAME}${NC}"
  echo -e "Path:             ${YELLOW}${DUSK_ROOT}${NC}"
  echo -e "Network:          ${YELLOW}${NETWORK}${NC}"
  echo -e "State number:     ${YELLOW}${state_number}${NC}"
  echo -e "${GREEN}========================================${NC}"
  echo ""
  echo -e "${YELLOW}WARNING: This will STOP ${SERVICE_NAME} and REPLACE${NC}"
  echo -e "${YELLOW}  ${DUSK_ROOT}/rusk/state${NC}"
  echo -e "${YELLOW}  ${DUSK_ROOT}/rusk/chain.db${NC}"
  echo ""

  while : ; do
    read -r -p "Are you sure you want to proceed? [y/N]: " choice
    choice=${choice,,}
    case "$choice" in
      y|yes)
        echo -e "${GREEN}Proceeding...${NC}"
        return 0
        ;;
      n|no|"")
        echo "Operation aborted by user."
        exit 1
        ;;
      *)
        echo "Please answer 'y' or 'n'."
        ;;
    esac
  done
}

list_states() {
  echo "Fetching available states from $STATE_LIST_URL..."
  if ! curl -f -L -s "$STATE_LIST_URL"; then
    echo -e "${RED}Error: Failed to fetch the list of states.${NC}"
    exit 1
  fi
}

state_exists() {
  local state=$1
  while : ; do
    if curl -f -L -s "$STATE_LIST_URL" | grep -q "^$state$"; then
      return 0
    else
      echo -e "${YELLOW}State '$state' does not exist on $NETWORK.${NC}"
      echo "Available states:"
      list_states
      read -p "Enter a valid state number (or Ctrl-C to abort): " state
      state_number=$state
    fi
  done
}

get_latest_state() {
  curl -f -L -s "$STATE_LIST_URL" | tail -n 1
}

#############################################
# Main flow
#############################################
if [[ "$LIST_STATES" == "1" ]]; then
  list_states
  exit 0
elif [[ -n "$STATE_NUMBER" ]]; then
  state_number=$STATE_NUMBER
  state_exists "$STATE_NUMBER"
else
  state_number=$(get_latest_state)
  if [[ -z "$state_number" ]]; then
    echo -e "${RED}Error: Could not determine latest state.${NC}"
    exit 1
  fi
fi

display_warning

STATE_URL="$STATE_BASE_URL/$state_number"
TMP_FILE="/tmp/state-${INSTANCE}-${state_number}.tar.gz"

echo -e "${GREEN}Downloading state $state_number from $STATE_URL...${NC}"
if ! curl -f -L -o "$TMP_FILE" "$STATE_URL"; then
  echo -e "${RED}Error: Download failed.${NC}"
  exit 1
fi

echo -e "${GREEN}Stopping ${SERVICE_NAME}...${NC}"
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  systemctl stop "${SERVICE_NAME}"
else
  echo "  (already stopped)"
fi

echo -e "${GREEN}Removing existing state and chain.db...${NC}"
rm -rf "${DUSK_ROOT}/rusk/state"
rm -rf "${DUSK_ROOT}/rusk/chain.db"

echo -e "${GREEN}Extracting new snapshot to ${DUSK_ROOT}/rusk/...${NC}"
tar -xf "$TMP_FILE" -C "${DUSK_ROOT}/rusk/"

echo -e "${GREEN}Setting ownership to dusk:dusk...${NC}"
chown -R dusk:dusk "${DUSK_ROOT}/rusk"

# Clean up
rm -f "$TMP_FILE"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}State download complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Snapshot:    $state_number"
echo "Instance:    ${INSTANCE}"
echo "Network:     ${NETWORK}"
echo ""
echo -e "${YELLOW}Next step:${NC} restart the service when ready:"
echo "  sudo systemctl start ${SERVICE_NAME}"
echo "  sudo journalctl -u ${SERVICE_NAME} -f"
echo ""
