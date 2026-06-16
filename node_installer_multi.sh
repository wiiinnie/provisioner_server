#!/usr/bin/env bash
set -e

#############################################
# Multi-Instance Dusk Node Installer (v4)
#
# Based on official upstream node-installer.sh v0.5.6
# (https://github.com/dusk-network/node-installer/releases/tag/v0.5.6)
# with multi-instance support layered on top.
#
# Key changes from v3:
#   - Hardcoded versions per network (rusk 1.7.0, rusk-wallet 0.3.0/0.4.0)
#   - Adds RUSK_CONSENSUS_SPIN_TIME to systemd service (CRITICAL for 1.7.0)
#   - Fixes URL construction (new dusk-rusk-{version} tag scheme since 1.0.0)
#   - Configures rusk-wallet ~/.dusk/rusk-wallet/config.toml (instance 1 only)
#   - Applies sysctl tuning via /etc/sysctl.d/dusk.conf (instance 1 only)
#   - Installs download_state utility (instance 1 only, mainnet/testnet)
#
# Multi-instance design:
#   - Per-instance: /opt/dusk{N}, rusk-{N}.service, /var/log/rusk-{N}.log,
#     Kadcast port = 9000+N, HTTP port = 8080+N (disabled by default)
#   - Shared (configured once on instance 1): dusk user/group, sysctl params,
#     ~/.dusk/rusk-wallet/config.toml, /opt/dusk/services/dusk.conf
#   - Skipped vs upstream: /usr/bin symlinks (would conflict between instances)
#############################################

#############################################
# Hardcoded versions (from upstream v0.5.6)
#############################################
declare -A VERSIONS
VERSIONS=(
    ["mainnet-rusk"]="1.7.0"
    ["mainnet-rusk-wallet"]="0.3.0"
    ["testnet-rusk"]="1.7.0"
    ["testnet-rusk-wallet"]="0.4.0"
    ["devnet-rusk"]="1.7.0"
    ["devnet-rusk-wallet"]="0.4.0"
)

#############################################
# Consensus spin times (CRITICAL for rusk 1.7.0)
#############################################
MAINNET_CONSENSUS_SPIN_TIME="1781175600"
TESTNET_CONSENSUS_SPIN_TIME="1779886800"
# devnet: no spin time set by upstream

# Default values
NETWORK="mainnet"
INSTANCE=1
FEATURE="default"
ARCH=$(uname -m)
KADCAST_BASE_PORT=9000

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

#############################################
# Help message
#############################################
show_help() {
    cat << EOF
Multi-Instance Dusk Node Installer (v4)

Based on upstream node-installer v0.5.6 with multi-instance support.

Usage: $0 [OPTIONS]

OPTIONS:
    --instance N           Instance number (default: 1)
                          Creates installation in /opt/dusk{N}
                          Uses Kadcast port 9000+N (e.g., 9001, 9002, 9003...)

    --network NETWORK      Network to install (default: mainnet)
                          Options: mainnet, testnet, devnet

    --feature FEATURE      Feature flag (default: default)
                          Options: default, archive

    -h, --help            Show this help message

EXAMPLES:
    # Install first instance on mainnet (default)
    sudo bash $0 --instance 1

    # Install second instance on mainnet
    sudo bash $0 --instance 2

    # Install testnet instance 3
    sudo bash $0 --instance 3 --network testnet

    # Install archive-feature instance
    sudo bash $0 --instance 4 --feature archive

VERSIONS INSTALLED (from upstream v0.5.6):
    rusk:           1.7.0  (all networks)
    rusk-wallet:    0.3.0  (mainnet)
                    0.4.0  (testnet, devnet)

NOTES:
    - Each instance gets a unique installation directory: /opt/dusk{N}
    - Kadcast ports: Instance 1 uses 9001, Instance 2 uses 9002, etc.
    - Standard ports (9000/8080) remain free for regular installations
    - HTTP is disabled by default (listen = false)
    - Service name: rusk-{N}
    - Log files: /var/log/rusk-{N}.log
    - Instance 1 sets up shared resources (sysctl, wallet config, etc.)

POST-INSTALL: For an upgrade scenario, you must manually wipe state
              before starting the service:
                  rm -rf /opt/dusk{N}/rusk/state /opt/dusk{N}/rusk/chain.db
EOF
    exit 0
}

#############################################
# Parse command line arguments
#############################################
while [[ $# -gt 0 ]]; do
    case $1 in
        --instance)
            INSTANCE="$2"
            shift 2
            ;;
        --network)
            NETWORK="$2"
            shift 2
            ;;
        --feature)
            FEATURE="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            ;;
    esac
done

#############################################
# Validation
#############################################
if ! [[ "$INSTANCE" =~ ^[0-9]+$ ]] || [ "$INSTANCE" -lt 1 ]; then
    echo -e "${RED}Error: Instance must be a positive integer${NC}"
    exit 1
fi

case "$NETWORK" in
    mainnet|testnet|devnet) ;;
    *)
        echo -e "${RED}Error: Network must be mainnet, testnet, or devnet${NC}"
        exit 1
        ;;
esac

case "$FEATURE" in
    default|archive) ;;
    *)
        echo -e "${RED}Error: Feature must be 'default' or 'archive'${NC}"
        exit 1
        ;;
esac

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
    exit 1
fi

#############################################
# Computed paths and ports
#############################################
KADCAST_PORT=$((KADCAST_BASE_PORT + INSTANCE))
HTTP_PORT=$((8080 + INSTANCE))

DUSK_ROOT="/opt/dusk${INSTANCE}"
SERVICE_NAME="rusk-${INSTANCE}"
LOG_FILE="/var/log/rusk-${INSTANCE}.log"
LOG_FILE_RECOVERY="/var/log/rusk-${INSTANCE}-recovery.log"

# Get current user (the one who invoked sudo)
CURRENT_USER="${SUDO_USER:-$USER}"
CURRENT_HOME=$(eval echo ~"$CURRENT_USER")

# Whether this is the "first" instance (controls shared resource setup)
IS_FIRST_INSTANCE=false
if [ ! -d "/opt/dusk" ] && [ ! -f "$CURRENT_HOME/.dusk/rusk-wallet/config.toml" ]; then
    IS_FIRST_INSTANCE=true
fi

# Architecture mapping
case "$ARCH" in
    x86_64)  ARCH="x64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)
        echo -e "${RED}Error: Unsupported architecture: $ARCH${NC}"
        exit 1
        ;;
esac

#############################################
# Banner
#############################################
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Multi-Instance Dusk Node Installer (v4)${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Instance Number:    ${YELLOW}${INSTANCE}${NC}"
echo -e "Network:            ${YELLOW}${NETWORK}${NC}"
echo -e "Feature:            ${YELLOW}${FEATURE}${NC}"
echo -e "Architecture:       ${YELLOW}${ARCH}${NC}"
echo -e "Installation Path:  ${YELLOW}${DUSK_ROOT}${NC}"
echo -e "Kadcast Port:       ${YELLOW}${KADCAST_PORT}/udp${NC}"
echo -e "HTTP Status:        ${YELLOW}Disabled${NC}"
echo -e "Service Name:       ${YELLOW}${SERVICE_NAME}${NC}"
echo -e "Log File:           ${YELLOW}${LOG_FILE}${NC}"
if [ "$IS_FIRST_INSTANCE" = true ]; then
    echo -e "Shared Setup:       ${YELLOW}Yes (first instance — sysctl, wallet, etc.)${NC}"
else
    echo -e "Shared Setup:       ${YELLOW}Skipped (not first instance)${NC}"
fi
echo ""
echo -e "Rusk Version:       ${YELLOW}${VERSIONS[${NETWORK}-rusk]}${NC}"
echo -e "Wallet Version:     ${YELLOW}${VERSIONS[${NETWORK}-rusk-wallet]}${NC}"
echo ""
echo -e "${YELLOW}NOTE: HTTP disabled by default. Enable in ${DUSK_ROOT}/conf/rusk.toml if needed.${NC}"
echo ""
read -p "Continue with installation? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Installation cancelled."
    exit 0
fi

#############################################
# OpenSSL version check (>= 3 required)
#############################################
echo -e "${GREEN}Checking OpenSSL version...${NC}"
if ! command -v openssl >/dev/null 2>&1 || [ "$(openssl version | awk '{print $2}' | cut -d. -f1)" -lt 3 ]; then
    echo -e "${RED}Error: OpenSSL 3 or higher is required.${NC}"
    echo "Please upgrade your OS or install a newer version of OpenSSL."
    exit 1
fi

#############################################
# Handle existing instance
#############################################
if [ -d "$DUSK_ROOT" ]; then
    echo -e "${YELLOW}Warning: Instance ${INSTANCE} already exists at ${DUSK_ROOT}${NC}"
    read -p "Do you want to upgrade/reinstall? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
    echo "Stopping existing service..."
    systemctl stop ${SERVICE_NAME} 2>/dev/null || true
fi

#############################################
# Create directory structure
#############################################
echo -e "${GREEN}Creating directory structure...${NC}"
mkdir -p ${DUSK_ROOT}/{bin,conf,services,installer,rusk}
mkdir -p ${DUSK_ROOT}/installer/os

#############################################
# Detect OS and source OS-specific script
#############################################
echo -e "${GREEN}Detecting OS...${NC}"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    distro=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
else
    echo -e "${RED}Error: Unable to detect OS. /etc/os-release not found.${NC}"
    exit 1
fi

case "$distro" in
    linuxmint*) distro="ubuntu" ;;
esac

echo "Detected OS: $ID"
echo "Normalized OS: $distro"
echo "Architecture: $ARCH"

#############################################
# Download installer package (OS scripts + configs)
#############################################
INSTALLER_URL="https://github.com/dusk-network/node-installer/tarball/main"
echo -e "${GREEN}Downloading installer package (OS scripts + configs)...${NC}"
curl -sL "$INSTALLER_URL" -o ${DUSK_ROOT}/installer/installer.tar.gz || {
    echo -e "${RED}Failed to download installer package${NC}"
    exit 1
}
tar xf ${DUSK_ROOT}/installer/installer.tar.gz --strip-components 1 --directory ${DUSK_ROOT}/installer

OS_SCRIPT="${DUSK_ROOT}/installer/os/${distro}.sh"
if [ -f "$OS_SCRIPT" ]; then
    echo -e "${GREEN}Loading OS-specific configuration for ${distro}...${NC}"
    source "$OS_SCRIPT"
else
    echo -e "${RED}Error: No OS support script found for ${distro}${NC}"
    echo "See: https://github.com/dusk-network/node-installer#contributing-os-support"
    exit 1
fi

#############################################
# Install dependencies (from OS script)
#############################################
if type install_deps >/dev/null 2>&1; then
    echo -e "${GREEN}Installing dependencies...${NC}"
    install_deps
else
    echo -e "${YELLOW}Warning: No install_deps function found in OS script${NC}"
fi

#############################################
# Create dusk user and group
#############################################
if ! getent group dusk >/dev/null 2>&1; then
    echo -e "${GREEN}Creating dusk group...${NC}"
    groupadd --system dusk
fi
if ! id -u dusk >/dev/null 2>&1; then
    echo -e "${GREEN}Creating dusk system user...${NC}"
    useradd --system --create-home --shell /usr/sbin/nologin --gid dusk dusk
fi

if ! id -nG "$CURRENT_USER" | grep -qw "dusk"; then
    echo -e "${GREEN}Adding user ${CURRENT_USER} to dusk group...${NC}"
    usermod -aG dusk "$CURRENT_USER"
    echo -e "${YELLOW}You may need to log out and back in for group changes to take effect.${NC}"
fi

#############################################
# Stop any active rusk service (per-instance)
#############################################
if systemctl is-active --quiet ${SERVICE_NAME}; then
    systemctl stop ${SERVICE_NAME}
    echo "Stopped ${SERVICE_NAME} service."
fi

#############################################
# Move installer scripts and configs into instance dir
#############################################
echo -e "${GREEN}Setting up instance ${INSTANCE} files...${NC}"
# Bin scripts (download_state.sh, setup_consensus_pwd.sh, etc.)
if [ -d "${DUSK_ROOT}/installer/bin" ]; then
    cp -f ${DUSK_ROOT}/installer/bin/* ${DUSK_ROOT}/bin/ 2>/dev/null || true
fi
# Configs (mainnet.toml, testnet.toml, wallet.toml, dusk.conf, etc.)
if [ -d "${DUSK_ROOT}/installer/conf" ]; then
    cp -f ${DUSK_ROOT}/installer/conf/* ${DUSK_ROOT}/conf/ 2>/dev/null || true
fi
# Services (rusk.service template)
if [ -d "${DUSK_ROOT}/installer/services" ]; then
    cp -n ${DUSK_ROOT}/installer/services/* ${DUSK_ROOT}/services/ 2>/dev/null || true
fi

#############################################
# install_component — matches upstream URL scheme
#############################################
install_component() {
    local component="$1"
    local key="${NETWORK}-${component}"
    local version="${VERSIONS[$key]}"

    if [[ -z "$version" ]]; then
        echo -e "${RED}Error: Version not found for $key${NC}"
        exit 1
    fi

    # Apply FEATURE suffix only for Rusk
    local feature_suffix=""
    local release_tag="${component}"
    if [[ "$component" == "rusk" ]]; then
        feature_suffix="-${FEATURE}"
        release_tag="dusk-rusk"
    fi

    # Strip any -rc.* suffix from the version (for download filename only)
    local sanitized_version="${version%-rc.*}"

    local url="https://github.com/dusk-network/rusk/releases/download/${release_tag}-${version}/${component}-${sanitized_version}-linux-${ARCH}${feature_suffix}.tar.gz"

    echo -e "${GREEN}Installing ${component} ${version} for ${NETWORK} (${ARCH}${feature_suffix})${NC}"
    echo "  URL: $url"

    local component_dir="${DUSK_ROOT}/installer/${component}"
    mkdir -p "$component_dir"

    curl -so ${DUSK_ROOT}/installer/${component}.tar.gz -L "$url" || {
        echo -e "${RED}Failed to download ${component}${NC}"
        echo "URL: $url"
        exit 1
    }
    tar xf ${DUSK_ROOT}/installer/${component}.tar.gz --strip-components 1 --directory "$component_dir"
}

#############################################
# Install rusk + rusk-wallet
#############################################
install_component "rusk"
if [ -f "${DUSK_ROOT}/installer/rusk/rusk" ]; then
    mv -f ${DUSK_ROOT}/installer/rusk/rusk ${DUSK_ROOT}/bin/
fi

install_component "rusk-wallet"
if [ -f "${DUSK_ROOT}/installer/rusk-wallet/rusk-wallet" ]; then
    mv -f ${DUSK_ROOT}/installer/rusk-wallet/rusk-wallet ${DUSK_ROOT}/bin/
fi

#############################################
# Configure rusk.toml: select network + disable HTTP
#############################################
echo -e "${GREEN}Configuring rusk.toml for ${NETWORK}...${NC}"

# Select the network-specific config + genesis files
case "$NETWORK" in
    mainnet)
        [ -f "${DUSK_ROOT}/conf/mainnet.genesis" ] && mv -f ${DUSK_ROOT}/conf/mainnet.genesis ${DUSK_ROOT}/conf/genesis.toml
        [ -f "${DUSK_ROOT}/conf/mainnet.toml" ]    && mv -f ${DUSK_ROOT}/conf/mainnet.toml    ${DUSK_ROOT}/conf/rusk.toml
        rm -f ${DUSK_ROOT}/conf/testnet.genesis ${DUSK_ROOT}/conf/testnet.toml
        rm -f ${DUSK_ROOT}/conf/devnet.genesis  ${DUSK_ROOT}/conf/devnet.toml
        ;;
    testnet)
        [ -f "${DUSK_ROOT}/conf/testnet.genesis" ] && mv -f ${DUSK_ROOT}/conf/testnet.genesis ${DUSK_ROOT}/conf/genesis.toml
        [ -f "${DUSK_ROOT}/conf/testnet.toml" ]    && mv -f ${DUSK_ROOT}/conf/testnet.toml    ${DUSK_ROOT}/conf/rusk.toml
        rm -f ${DUSK_ROOT}/conf/mainnet.genesis ${DUSK_ROOT}/conf/mainnet.toml
        rm -f ${DUSK_ROOT}/conf/devnet.genesis  ${DUSK_ROOT}/conf/devnet.toml
        ;;
    devnet)
        [ -f "${DUSK_ROOT}/conf/devnet.genesis" ] && mv -f ${DUSK_ROOT}/conf/devnet.genesis ${DUSK_ROOT}/conf/genesis.toml
        [ -f "${DUSK_ROOT}/conf/devnet.toml" ]    && mv -f ${DUSK_ROOT}/conf/devnet.toml    ${DUSK_ROOT}/conf/rusk.toml
        rm -f ${DUSK_ROOT}/conf/mainnet.genesis ${DUSK_ROOT}/conf/mainnet.toml
        rm -f ${DUSK_ROOT}/conf/testnet.genesis ${DUSK_ROOT}/conf/testnet.toml
        ;;
esac

#############################################
# Rewrite instance-specific paths in rusk.toml
#
# The upstream-provided <network>.toml hardcodes:
#   db_path             = '/opt/dusk/rusk'
#   consensus_keys_path = '/opt/dusk/conf/consensus.keys'
# Both must point at the per-instance directory or rusk will fall back to
# the user-home default (~/.dusk/rusk/state → /root/.dusk/rusk/state for
# service running as root) and fail to find state.
#############################################
echo -e "${GREEN}Rewriting instance-specific paths in rusk.toml...${NC}"
sed -i \
    -e "s|^db_path = '/opt/dusk/rusk'|db_path = '${DUSK_ROOT}/rusk'|" \
    -e "s|^consensus_keys_path = '/opt/dusk/conf/consensus.keys'|consensus_keys_path = '${DUSK_ROOT}/conf/consensus.keys'|" \
    ${DUSK_ROOT}/conf/rusk.toml

# Verify the rewrite landed
if ! grep -q "^db_path = '${DUSK_ROOT}/rusk'" ${DUSK_ROOT}/conf/rusk.toml; then
    echo -e "${YELLOW}Warning: db_path rewrite may not have matched expected pattern${NC}"
    echo "Current db_path line in rusk.toml:"
    grep "^db_path" ${DUSK_ROOT}/conf/rusk.toml || echo "  (not found)"
fi
if ! grep -q "^consensus_keys_path = '${DUSK_ROOT}/conf/consensus.keys'" ${DUSK_ROOT}/conf/rusk.toml; then
    echo -e "${YELLOW}Warning: consensus_keys_path rewrite may not have matched expected pattern${NC}"
    echo "Current consensus_keys_path line in rusk.toml:"
    grep "^consensus_keys_path" ${DUSK_ROOT}/conf/rusk.toml || echo "  (not found)"
fi

# Disable HTTP (multi-instance: avoid port conflicts)
cat >> ${DUSK_ROOT}/conf/rusk.toml << EOF

# HTTP Configuration — Disabled for multi-instance setup
# To enable HTTP, set listen=true and configure listen_address (e.g. 0.0.0.0:${HTTP_PORT})
[http]
listen = false
EOF

#############################################
# Per-instance Kadcast configuration
#############################################
echo -e "${GREEN}Configuring Kadcast for port ${KADCAST_PORT}...${NC}"
cat > ${DUSK_ROOT}/services/rusk.conf.user << EOF
# Kadcast configuration for instance ${INSTANCE}
# Replace the public address with your VPS public IP after install:
#   echo 'KADCAST_PUBLIC_ADDRESS=YOUR_PUBLIC_IP:${KADCAST_PORT}' | sudo tee ${DUSK_ROOT}/services/rusk.conf.user
KADCAST_PUBLIC_ADDRESS=0.0.0.0:${KADCAST_PORT}
KADCAST_LISTEN_ADDRESS=0.0.0.0:${KADCAST_PORT}
EOF

#############################################
# Build systemd service file (with RUSK_CONSENSUS_SPIN_TIME)
#############################################
echo -e "${GREEN}Creating systemd service ${SERVICE_NAME}.service...${NC}"

# Pick spin time for this network
SPIN_TIME=""
case "$NETWORK" in
    mainnet) SPIN_TIME="$MAINNET_CONSENSUS_SPIN_TIME" ;;
    testnet) SPIN_TIME="$TESTNET_CONSENSUS_SPIN_TIME" ;;
    # devnet: upstream doesn't set one
esac

cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=Dusk Rusk Node — Instance ${INSTANCE} (${NETWORK})
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${DUSK_ROOT}
ExecStart=${DUSK_ROOT}/bin/rusk --config ${DUSK_ROOT}/conf/rusk.toml --profile ${DUSK_ROOT}/rusk
StandardOutput=append:${LOG_FILE}
StandardError=append:${LOG_FILE}
Restart=always
RestartSec=10
TimeoutStopSec=45
KillSignal=SIGTERM
SendSIGKILL=yes

Environment="NETWORK=${NETWORK}"
Environment="RUSK_RECOVERY_INPUT=${DUSK_ROOT}/conf/genesis.toml"
EOF

# Add RUSK_CONSENSUS_SPIN_TIME if set for this network (mainnet/testnet)
if [ -n "$SPIN_TIME" ]; then
    cat >> /etc/systemd/system/${SERVICE_NAME}.service << EOF
Environment="RUSK_CONSENSUS_SPIN_TIME=${SPIN_TIME}"
EOF
fi

cat >> /etc/systemd/system/${SERVICE_NAME}.service << EOF

# Shared dusk config (DUSK_CONSENSUS_KEYS_PASS, etc.)
EnvironmentFile=-/opt/dusk/services/dusk.conf
# Per-instance Kadcast addresses
EnvironmentFile=-${DUSK_ROOT}/services/rusk.conf.user

[Install]
WantedBy=multi-user.target
EOF

#############################################
# Set permissions
#############################################
echo -e "${GREEN}Setting permissions...${NC}"
chown -R dusk:dusk ${DUSK_ROOT}
chmod -R 660 ${DUSK_ROOT}
find ${DUSK_ROOT} -type d -exec chmod +x {} \;
chmod +x ${DUSK_ROOT}/bin/*

#############################################
# Log files
#############################################
touch ${LOG_FILE} ${LOG_FILE_RECOVERY}
chown dusk:dusk ${LOG_FILE} ${LOG_FILE_RECOVERY}
chmod 664 ${LOG_FILE} ${LOG_FILE_RECOVERY}

#############################################
# Logrotate (per-instance)
#
# Note: we DO NOT call the upstream OS script's configure_logrotate() —
# it hardcodes the source path as /opt/dusk/services/logrotate.conf which
# doesn't exist in our multi-instance layout, AND it installs for service
# name 'rusk' instead of our 'rusk-{N}'. We write our own config instead.
#############################################
echo -e "${GREEN}Configuring log rotation for ${SERVICE_NAME}...${NC}"
cat > /etc/logrotate.d/${SERVICE_NAME} << EOF
${LOG_FILE} {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0664 dusk dusk
}

${LOG_FILE_RECOVERY} {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0664 dusk dusk
}
EOF

#############################################
# SHARED RESOURCES (instance 1 only)
#
# These are configured globally and shared between all instances.
# Skipped for subsequent instances to avoid clobbering existing setup.
#############################################
if [ "$IS_FIRST_INSTANCE" = true ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Configuring shared resources (first instance)${NC}"
    echo -e "${GREEN}========================================${NC}"

    # Shared /opt/dusk dir for shared configs
    mkdir -p /opt/dusk/services

    # ── sysctl tuning (UDP buffers, etc. for Kadcast) ─────────────────
    if [ -f "${DUSK_ROOT}/conf/dusk.conf" ]; then
        echo -e "${GREEN}Installing sysctl parameters → /etc/sysctl.d/dusk.conf...${NC}"
        cp -f ${DUSK_ROOT}/conf/dusk.conf /etc/sysctl.d/dusk.conf
        sysctl -p /etc/sysctl.d/dusk.conf || echo -e "${YELLOW}sysctl -p had warnings (non-fatal)${NC}"
    else
        echo -e "${YELLOW}No sysctl config file (${DUSK_ROOT}/conf/dusk.conf) — skipping${NC}"
    fi

    # ── rusk-wallet config (~/.dusk/rusk-wallet/config.toml) ──────────
    mkdir -p $CURRENT_HOME/.dusk/rusk-wallet
    chown -R "$CURRENT_USER:dusk" "$CURRENT_HOME/.dusk"
    chmod -R 770 "$CURRENT_HOME/.dusk"

    if [ -f "${DUSK_ROOT}/conf/wallet.toml" ]; then
        echo -e "${GREEN}Installing rusk-wallet config → ${CURRENT_HOME}/.dusk/rusk-wallet/config.toml...${NC}"
        cp -f ${DUSK_ROOT}/conf/wallet.toml $CURRENT_HOME/.dusk/rusk-wallet/config.toml

        # Set the prover URL per network
        case "$NETWORK" in
            mainnet) PROVER_URL="https://provers.dusk.network" ;;
            testnet) PROVER_URL="https://testnet.provers.dusk.network" ;;
            devnet)  PROVER_URL="https://devnet.provers.dusk.network" ;;
        esac
        sed -i "s|^prover = .*|prover = \"$PROVER_URL\"|" $CURRENT_HOME/.dusk/rusk-wallet/config.toml
        chown "$CURRENT_USER:dusk" $CURRENT_HOME/.dusk/rusk-wallet/config.toml
    else
        echo -e "${YELLOW}No wallet.toml in installer package — skipping wallet config${NC}"
    fi

    # ── download_state utility (mainnet/testnet only) ─────────────────
    if [[ "$NETWORK" == "mainnet" || "$NETWORK" == "testnet" ]]; then
        if [ -f "${DUSK_ROOT}/bin/download_state.sh" ]; then
            echo -e "${GREEN}Installing download_state utility → /usr/bin/download_state${NC}"
            ln -sf ${DUSK_ROOT}/bin/download_state.sh /usr/bin/download_state
        fi
    fi

    # ── Shared dusk.conf for password (created empty if missing) ──────
    if [ ! -f /opt/dusk/services/dusk.conf ]; then
        echo -e "${GREEN}Creating /opt/dusk/services/dusk.conf (shared password file)...${NC}"
        touch /opt/dusk/services/dusk.conf
        chmod 640 /opt/dusk/services/dusk.conf
        chown root:dusk /opt/dusk/services/dusk.conf
        cat > /opt/dusk/services/dusk.conf << EOF
# Shared Dusk service environment (used by ALL rusk-{N} instances)
# Set the consensus keys password here:
# DUSK_CONSENSUS_KEYS_PASS=your_password_here
EOF
    else
        echo -e "${YELLOW}/opt/dusk/services/dusk.conf already exists — leaving untouched${NC}"
    fi
else
    echo -e "${YELLOW}Skipping shared resource setup (not first instance)${NC}"
fi

#############################################
# Reload systemd
#############################################
systemctl daemon-reload

#############################################
# Cleanup
#############################################
rm -rf ${DUSK_ROOT}/installer/installer.tar.gz
rm -rf ${DUSK_ROOT}/installer/rusk.tar.gz
rm -rf ${DUSK_ROOT}/installer/rusk-wallet.tar.gz

#############################################
# Final instructions
#############################################
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Versions installed:${NC}"
echo -e "  rusk:        ${VERSIONS[${NETWORK}-rusk]}"
echo -e "  rusk-wallet: ${VERSIONS[${NETWORK}-rusk-wallet]}"
if [ -n "$SPIN_TIME" ]; then
    echo -e "  RUSK_CONSENSUS_SPIN_TIME: ${SPIN_TIME}"
fi
echo ""
echo -e "${YELLOW}IMPORTANT — Firewall:${NC}"
echo "  ufw allow ${KADCAST_PORT}/udp     # (or iptables equivalent)"
echo ""
echo -e "${YELLOW}IMPORTANT — Upgrade scenario (state wipe required):${NC}"
echo "  If this is an upgrade (e.g., post-BOREAS hardfork), wipe state first:"
echo "    sudo rm -rf ${DUSK_ROOT}/rusk/state ${DUSK_ROOT}/rusk/chain.db"
echo ""
echo -e "${YELLOW}NEXT STEPS:${NC}"
echo ""
echo "1. Update Kadcast public address (replace YOUR_PUBLIC_IP):"
echo "   echo 'KADCAST_PUBLIC_ADDRESS=YOUR_PUBLIC_IP:${KADCAST_PORT}' | sudo tee ${DUSK_ROOT}/services/rusk.conf.user"
echo "   echo 'KADCAST_LISTEN_ADDRESS=0.0.0.0:${KADCAST_PORT}' | sudo tee -a ${DUSK_ROOT}/services/rusk.conf.user"
echo ""
echo "2. Export and set up consensus keys (per instance):"
echo "   sozu-wallet -w ~/sozu_provisioner --password 'YOUR_PW' export --profile-idx $((INSTANCE-1)) --dir ${DUSK_ROOT}/conf --name consensus"
echo ""
if [ "$IS_FIRST_INSTANCE" = true ]; then
echo "3. Set consensus keys password in shared dusk.conf:"
echo "   echo 'DUSK_CONSENSUS_KEYS_PASS=your_password' | sudo tee -a /opt/dusk/services/dusk.conf"
echo ""
fi
echo "4. Enable and start the service:"
echo "   sudo systemctl enable ${SERVICE_NAME}"
echo "   sudo systemctl start ${SERVICE_NAME}"
echo ""
echo "5. Watch logs:"
echo "   sudo journalctl -u ${SERVICE_NAME} -f"
echo "   # OR"
echo "   tail -f ${LOG_FILE}"
echo ""
echo -e "${YELLOW}Installation paths:${NC}"
echo -e "  Instance dir:   ${DUSK_ROOT}"
echo -e "  Service:        ${SERVICE_NAME}"
echo -e "  Service file:   /etc/systemd/system/${SERVICE_NAME}.service"
echo -e "  Kadcast port:   ${KADCAST_PORT}/udp"
echo -e "  HTTP:           Disabled (enable in ${DUSK_ROOT}/conf/rusk.toml if needed)"
echo ""
echo -e "${GREEN}========================================${NC}"
