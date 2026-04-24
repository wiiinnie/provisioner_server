#!/usr/bin/env bash
set -e

#############################################
# Multi-Instance Dusk Node Installer
#
# Adapted from upstream:
#   https://raw.githubusercontent.com/dusk-network/node-installer/main/node-installer.sh
#
# Key differences from upstream:
#   - Installation path is /opt/dusk${INSTANCE}/ instead of /opt/dusk/
#   - Each instance has its own systemd unit (rusk-${INSTANCE}.service)
#   - Each instance uses a different Kadcast port (9000 + INSTANCE)
#   - HTTP is disabled by default (no port conflicts)
#   - No system-wide symlinks (which instance's rusk would /usr/bin/rusk point to?)
#   - No rusk-wallet config setup (sozu-wallet replaces it for operator use)
#   - db_path and consensus_keys_path rewritten to per-instance values
#   - /opt/dusk{N}/services/dusk.conf per instance (each must contain
#     DUSK_CONSENSUS_KEYS_PASS; operator creates post-install)
#############################################

# Default values
NETWORK="mainnet"
FEATURE="default"
INSTANCE=1
KADCAST_BASE_PORT=9000

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Help message
show_help() {
    cat << EOF
Multi-Instance Dusk Node Installer

Usage: sudo bash $0 [OPTIONS]

OPTIONS:
    --instance N           Instance number (default: 1)
                          Installs to /opt/dusk{N}
                          Uses Kadcast port 9000+N (e.g., 9001 for instance 1)

    --network NETWORK      Network (default: mainnet)
                          Options: mainnet, testnet

    --feature FEATURE      Rusk feature variant (default: default)
                          Options: default (Provisioner node), archive

    -h, --help            Show this help

EXAMPLES:
    sudo bash $0 --instance 1 --network mainnet
    sudo bash $0 --instance 2 --network testnet
    sudo bash $0 --instance 3 --feature archive

NOTES:
    - Standard ports 9000 and 8080 remain free for the canonical single-instance install
    - HTTP is disabled (listen = false) — use --http-listen-addr or edit rusk.toml to enable
    - Service name: rusk-{N}, log files: /var/log/rusk-{N}.log
    - /opt/dusk{N}/services/dusk.conf (holds DUSK_CONSENSUS_KEYS_PASS)
      is created by the operator post-install; see NEXT STEPS

EOF
    exit 0
}

# Parse args
while [[ "$#" -gt 0 ]]; do
    case "$1" in
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

# Validate
if ! [[ "$INSTANCE" =~ ^[0-9]+$ ]] || [ "$INSTANCE" -lt 1 ]; then
    echo -e "${RED}Error: Instance must be a positive integer${NC}"
    exit 1
fi

case "$NETWORK" in
    mainnet|testnet)
        ;;
    *)
        echo -e "${RED}Error: Unknown network $NETWORK. Use 'mainnet' or 'testnet'.${NC}"
        exit 1
        ;;
esac

case "$FEATURE" in
    default|archive)
        ;;
    *)
        echo -e "${RED}Error: Unknown feature $FEATURE. Use 'default' or 'archive'.${NC}"
        exit 1
        ;;
esac

# Must be root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
    exit 1
fi

# Detect invoking user
CURRENT_USER=$(logname 2>/dev/null || echo "${SUDO_USER:-root}")

# Per-instance paths
DUSK_ROOT="/opt/dusk${INSTANCE}"
SERVICE_NAME="rusk-${INSTANCE}"
LOG_FILE="/var/log/rusk-${INSTANCE}.log"
LOG_FILE_RECOVERY="/var/log/rusk-${INSTANCE}-recovery.log"
KADCAST_PORT=$((KADCAST_BASE_PORT + INSTANCE))
HTTP_PORT=$((8080 + INSTANCE))

# Architecture
case "$(uname -m)" in
    x86_64) ARCH="x64";;
    aarch64|arm64) ARCH="arm64";;
    *) echo -e "${RED}Unsupported architecture: $(uname -m)${NC}"; exit 1;;
esac

# ── Display plan and confirm ──────────────────────────────────────────────────
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Multi-Instance Dusk Node Installer${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Instance:          ${YELLOW}${INSTANCE}${NC}"
echo -e "Network:           ${YELLOW}${NETWORK}${NC}"
echo -e "Feature:           ${YELLOW}${FEATURE}${NC}"
echo -e "Architecture:      ${YELLOW}${ARCH}${NC}"
echo -e "Install directory: ${YELLOW}${DUSK_ROOT}${NC}"
echo -e "Service name:      ${YELLOW}${SERVICE_NAME}${NC}"
echo -e "Kadcast port:      ${YELLOW}${KADCAST_PORT}/udp${NC}"
echo -e "HTTP:              ${YELLOW}Disabled${NC}"
echo -e "Log file:          ${YELLOW}${LOG_FILE}${NC}"
echo ""

read -rp "Continue with installation? (y/N) " -n 1
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Installation cancelled."
    exit 0
fi

# Handle existing instance
if [ -d "$DUSK_ROOT" ]; then
    echo -e "${YELLOW}Warning: Instance ${INSTANCE} already exists at ${DUSK_ROOT}${NC}"
    read -rp "Overwrite / reinstall? (y/N) " -n 1
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
    echo "Stopping existing service..."
    systemctl stop ${SERVICE_NAME} 2>/dev/null || true
fi

# ── Prerequisites ─────────────────────────────────────────────────────────────

# Check OpenSSL 3+
if ! command -v openssl >/dev/null 2>&1 || [ "$(openssl version | awk '{print $2}' | cut -d. -f1)" -lt 3 ]; then
    echo -e "${RED}Error: OpenSSL 3 or higher is required${NC}"
    exit 1
fi

# ── Directory setup ───────────────────────────────────────────────────────────
echo -e "${GREEN}Creating directory structure...${NC}"
rm -rf ${DUSK_ROOT}/installer || true
mkdir -p ${DUSK_ROOT}/{bin,conf,rusk,services,installer}

# ── Download installer tarball (contains bin/conf/services/os/assets) ─────────
echo -e "${GREEN}Downloading installer package...${NC}"
INSTALLER_URL="https://github.com/dusk-network/node-installer/tarball/main"
curl -so ${DUSK_ROOT}/installer/installer.tar.gz -L "$INSTALLER_URL" || {
    echo -e "${RED}Failed to download installer package${NC}"
    exit 1
}
tar xf ${DUSK_ROOT}/installer/installer.tar.gz --strip-components 1 --directory ${DUSK_ROOT}/installer

# ── OS-specific bootstrap ─────────────────────────────────────────────────────
if [ -f /etc/os-release ]; then
    . /etc/os-release
    distro=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
else
    echo -e "${RED}Error: Unable to detect OS${NC}"
    exit 1
fi

case "$distro" in
    linuxmint*) distro="ubuntu" ;;
esac

echo -e "${GREEN}Detected OS: $ID (normalized: $distro)${NC}"

OS_SCRIPT="${DUSK_ROOT}/installer/os/${distro}.sh"
if [ -f "$OS_SCRIPT" ]; then
    source "$OS_SCRIPT"
else
    echo -e "${RED}Error: No OS support script for '$distro'${NC}"
    echo "Supported distros: see ${DUSK_ROOT}/installer/os/"
    exit 1
fi

echo -e "${GREEN}Installing system prerequisites...${NC}"
install_deps

# ── dusk user/group ───────────────────────────────────────────────────────────
# Check group and user independently — previous partial installs may have
# created only the group.
if ! getent group dusk >/dev/null 2>&1; then
    echo -e "${GREEN}Creating dusk system group...${NC}"
    groupadd --system dusk
fi

if ! id -u dusk >/dev/null 2>&1; then
    echo -e "${GREEN}Creating dusk system user...${NC}"
    useradd --system --create-home --shell /usr/sbin/nologin --gid dusk dusk
fi

if ! id -nG "$CURRENT_USER" | grep -qw "dusk"; then
    usermod -aG dusk "$CURRENT_USER"
    echo -e "${YELLOW}User $CURRENT_USER added to dusk group. Log out and back in for changes to take effect.${NC}"
fi

# ── Install helper scripts and conf files from tarball ────────────────────────
echo -e "${GREEN}Installing helper scripts and configs...${NC}"
if [ -d ${DUSK_ROOT}/installer/bin ]; then
    cp -rf ${DUSK_ROOT}/installer/bin/* ${DUSK_ROOT}/bin/
fi
if [ -d ${DUSK_ROOT}/installer/conf ]; then
    cp -rf ${DUSK_ROOT}/installer/conf/* ${DUSK_ROOT}/conf/
fi
if [ -d ${DUSK_ROOT}/installer/services ]; then
    cp -rnf ${DUSK_ROOT}/installer/services/* ${DUSK_ROOT}/services/ 2>/dev/null || \
        cp -rf ${DUSK_ROOT}/installer/services/* ${DUSK_ROOT}/services/
fi

# ── Download and install rusk + rusk-wallet ───────────────────────────────────
#
# Dusk uses different release streams on the same repo:
#   rusk:        tag "dusk-rusk-<ver>", asset "rusk-<ver>-linux-<arch>-<feature>.tar.gz"
#   rusk-wallet: tag "rusk-wallet-<ver>", asset "rusk-wallet-<ver>-linux-<arch>.tar.gz"
# Each component has its own tag cadence.

install_component() {
    local component="$1"
    local tag=""
    local ver=""
    local url=""

    case "$component" in
        rusk)
            tag=$(curl -s https://api.github.com/repos/dusk-network/rusk/releases \
                  | grep '"tag_name"' \
                  | grep 'dusk-rusk-' \
                  | head -1 \
                  | cut -d'"' -f4)
            if [ -z "$tag" ]; then
                echo -e "${RED}Error: Could not fetch latest rusk release tag${NC}"
                exit 1
            fi
            ver="${tag#dusk-rusk-}"
            ver="${ver#v}"
            url="https://github.com/dusk-network/rusk/releases/download/${tag}/${component}-${ver}-linux-${ARCH}-${FEATURE}.tar.gz"
            ;;
        rusk-wallet)
            tag=$(curl -s https://api.github.com/repos/dusk-network/rusk/releases \
                  | grep '"tag_name"' \
                  | grep 'rusk-wallet-' \
                  | head -1 \
                  | cut -d'"' -f4)
            if [ -z "$tag" ]; then
                echo -e "${RED}Error: Could not fetch latest rusk-wallet release tag${NC}"
                exit 1
            fi
            ver="${tag#rusk-wallet-}"
            url="https://github.com/dusk-network/rusk/releases/download/${tag}/${component}-${ver}-linux-${ARCH}.tar.gz"
            ;;
        *)
            echo -e "${RED}Unknown component: $component${NC}"
            exit 1
            ;;
    esac

    echo -e "${GREEN}Downloading ${component} (${tag})...${NC}"

    local component_dir="${DUSK_ROOT}/installer/${component}"
    mkdir -p "$component_dir"
    curl -sfL "$url" -o ${DUSK_ROOT}/installer/${component}.tar.gz || {
        echo -e "${RED}Failed to download ${component}${NC}"
        echo "URL: $url"
        exit 1
    }
    tar xf ${DUSK_ROOT}/installer/${component}.tar.gz --strip-components 1 --directory "$component_dir"
    # Move just the binary into our bin dir (the tarball also has README/CHANGELOG)
    if [ -f "${component_dir}/${component}" ]; then
        mv -f "${component_dir}/${component}" ${DUSK_ROOT}/bin/
    fi
}

install_component "rusk"
install_component "rusk-wallet"

# ── Configure network (select mainnet or testnet config) ──────────────────────
echo -e "${GREEN}Configuring network: ${NETWORK}${NC}"
if [ -f "${DUSK_ROOT}/conf/${NETWORK}.toml" ]; then
    mv -f "${DUSK_ROOT}/conf/${NETWORK}.toml" "${DUSK_ROOT}/conf/rusk.toml"
else
    echo -e "${RED}Error: ${DUSK_ROOT}/conf/${NETWORK}.toml not found in installer package${NC}"
    exit 1
fi
if [ -f "${DUSK_ROOT}/conf/${NETWORK}.genesis" ]; then
    mv -f "${DUSK_ROOT}/conf/${NETWORK}.genesis" "${DUSK_ROOT}/conf/genesis.toml"
fi
# Clean up the other network's conf files
rm -f ${DUSK_ROOT}/conf/mainnet.toml ${DUSK_ROOT}/conf/mainnet.genesis \
      ${DUSK_ROOT}/conf/testnet.toml ${DUSK_ROOT}/conf/testnet.genesis

# ── Rewrite paths in rusk.toml for per-instance layout ────────────────────────
echo -e "${GREEN}Rewriting paths in rusk.toml for instance ${INSTANCE}...${NC}"
sed -i "s|^db_path\s*=\s*'/opt/dusk/|db_path = '${DUSK_ROOT}/|" ${DUSK_ROOT}/conf/rusk.toml
sed -i "s|^consensus_keys_path\s*=\s*'/opt/dusk/|consensus_keys_path = '${DUSK_ROOT}/|" ${DUSK_ROOT}/conf/rusk.toml
echo "Per-instance paths:"
grep -E "^db_path|^consensus_keys_path" ${DUSK_ROOT}/conf/rusk.toml | sed 's/^/  /'

# ── Rewrite paths in helper scripts (check_consensus_keys.sh, download_state.sh) ──
# check_consensus_keys.sh references paths like /opt/dusk/conf/rusk.toml and
# /opt/dusk/services/dusk.conf. Per testnet layout, both should be per-instance
# (/opt/dusk${INSTANCE}/). A separate dusk.conf must exist per instance (the
# installer does NOT create this — operator does it post-install, see NEXT STEPS).
if [ -f "${DUSK_ROOT}/bin/check_consensus_keys.sh" ]; then
    sed -i "s|/opt/dusk/|${DUSK_ROOT}/|g" "${DUSK_ROOT}/bin/check_consensus_keys.sh"
fi

# download_state.sh (fast-sync snapshot tool, mainnet only) also hardcodes
# /opt/dusk/ paths and stops the canonical "rusk" service. Rewrite for this
# instance so it targets /opt/dusk${INSTANCE}/ and rusk-${INSTANCE}.service,
# and uses an instance-specific /tmp file to avoid collisions when running
# in parallel across instances.
if [ -f "${DUSK_ROOT}/bin/download_state.sh" ]; then
    sed -i "s|/opt/dusk/|${DUSK_ROOT}/|g" "${DUSK_ROOT}/bin/download_state.sh"
    sed -i "s|service rusk stop|systemctl stop ${SERVICE_NAME}|" "${DUSK_ROOT}/bin/download_state.sh"
    sed -i "s|/tmp/state.tar.gz|/tmp/state-${INSTANCE}.tar.gz|g" "${DUSK_ROOT}/bin/download_state.sh"
fi

# ── Append HTTP=disabled to rusk.toml ─────────────────────────────────────────
cat >> ${DUSK_ROOT}/conf/rusk.toml << EOF

# HTTP Configuration - Disabled for multi-instance setup
[http]
listen = false
EOF

# ── Configure Kadcast port via rusk.conf.user ─────────────────────────────────
echo -e "${GREEN}Configuring Kadcast port ${KADCAST_PORT}...${NC}"
cat > ${DUSK_ROOT}/services/rusk.conf.user << EOF
# Kadcast configuration for instance ${INSTANCE}
# Replace 0.0.0.0 with your public IP for proper peer discovery.
KADCAST_PUBLIC_ADDRESS=0.0.0.0:${KADCAST_PORT}
KADCAST_LISTEN_ADDRESS=0.0.0.0:${KADCAST_PORT}
EOF

# ── Systemd service ───────────────────────────────────────────────────────────
# Modeled on the upstream rusk.service template but with per-instance paths.
# Key parts:
#   RUSK_PROFILE_PATH     — per-instance profile dir (state, keys, circuits)
#   RUSK_RECOVERY_INPUT   — points at genesis.toml for "rusk recovery state"
#   ExecStartPre chain    — idempotent state init, key sanity, ownership fix
#   EnvironmentFile chain — dusk.conf (per-instance password), rusk.conf.user (ports)
echo -e "${GREEN}Creating systemd service ${SERVICE_NAME}...${NC}"
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=DUSK Rusk - Instance ${INSTANCE}
After=network.target

[Service]
Type=simple

Environment="RUST_BACKTRACE=full"
Environment="RUSK_PROFILE_PATH=${DUSK_ROOT}/rusk"
Environment="RUSK_RECOVERY_INPUT=${DUSK_ROOT}/conf/genesis.toml"

User=root
WorkingDirectory=${DUSK_ROOT}

ExecStartPre=!/bin/bash -c '${DUSK_ROOT}/bin/rusk recovery state >> ${LOG_FILE_RECOVERY} 2>&1'
ExecStartPre=!/bin/bash -c '${DUSK_ROOT}/bin/check_consensus_keys.sh'
ExecStartPre=!/bin/bash -c 'chown -R dusk ${DUSK_ROOT}/rusk/state 2>/dev/null || true'

EnvironmentFile=${DUSK_ROOT}/services/dusk.conf
EnvironmentFile=-${DUSK_ROOT}/services/rusk.conf.user

ExecStart=${DUSK_ROOT}/bin/rusk \\
            --config ${DUSK_ROOT}/conf/rusk.toml

StandardOutput=append:${LOG_FILE}
StandardError=append:${LOG_FILE}

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# ── Permissions ───────────────────────────────────────────────────────────────
echo -e "${GREEN}Setting permissions...${NC}"
chown -R dusk:dusk ${DUSK_ROOT}
chmod -R 770 ${DUSK_ROOT}
chmod +x ${DUSK_ROOT}/bin/*

# Log files
touch ${LOG_FILE} ${LOG_FILE_RECOVERY}
chown dusk:dusk ${LOG_FILE} ${LOG_FILE_RECOVERY}
chmod 664 ${LOG_FILE} ${LOG_FILE_RECOVERY}

# ── Logrotate (per-instance) ──────────────────────────────────────────────────
echo -e "${GREEN}Configuring logrotate for ${SERVICE_NAME}...${NC}"
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

# ── System params (sysctl) — apply once, shared across instances ──────────────
if [ -f ${DUSK_ROOT}/conf/dusk.conf ]; then
    if [ ! -f /etc/sysctl.d/dusk.conf ]; then
        echo -e "${GREEN}Applying system parameters (sysctl)...${NC}"
        cp -f ${DUSK_ROOT}/conf/dusk.conf /etc/sysctl.d/dusk.conf
        sysctl -p /etc/sysctl.d/dusk.conf >/dev/null 2>&1 || true
    fi
    rm -f ${DUSK_ROOT}/conf/dusk.conf
fi

# ── Finalize ──────────────────────────────────────────────────────────────────
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}

# Clean up installer tarball
rm -rf ${DUSK_ROOT}/installer

# ── Done — next steps ─────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}FIREWALL:${NC}"
echo "  sudo ufw allow ${KADCAST_PORT}/udp    # Kadcast for instance ${INSTANCE}"
echo ""
echo -e "${YELLOW}NEXT STEPS:${NC}"
echo ""
echo "1. Export consensus keys for this instance (run ONCE per instance,"
echo "   from a box where sozu_provisioner wallet exists):"
echo ""
echo "   sozu-wallet -w ~/sozu_provisioner --password 'YOUR_PASSWORD' \\"
echo "       export --profile-idx $((INSTANCE-1)) \\"
echo "       --dir ${DUSK_ROOT}/conf --name consensus"
echo ""
echo "2. Set the consensus keys password (run ONCE per instance):"
echo ""
echo "   echo 'DUSK_CONSENSUS_KEYS_PASS=your_password' | sudo tee ${DUSK_ROOT}/services/dusk.conf"
echo "   sudo chmod 600 ${DUSK_ROOT}/services/dusk.conf"
echo ""
echo "   (Same password across instances is fine — each one just needs the file"
echo "    at its own ${DUSK_ROOT}/services/dusk.conf path.)"
echo ""
echo "3. Update Kadcast public address (replace YOUR_PUBLIC_IP):"
echo ""
echo "   sudo sed -i 's|0.0.0.0:${KADCAST_PORT}|YOUR_PUBLIC_IP:${KADCAST_PORT}|' \\"
echo "       ${DUSK_ROOT}/services/rusk.conf.user"
echo ""
echo "4. Start the service:"
echo ""
echo "   sudo systemctl start ${SERVICE_NAME}"
echo "   sudo systemctl status ${SERVICE_NAME}"
echo "   tail -f ${LOG_FILE}"
echo ""
echo -e "${YELLOW}Installation directory:${NC}  ${DUSK_ROOT}"
echo -e "${YELLOW}Service name:${NC}            ${SERVICE_NAME}"
echo -e "${YELLOW}Kadcast port:${NC}            ${KADCAST_PORT}/udp"
echo -e "${YELLOW}HTTP:${NC}                    Disabled (edit ${DUSK_ROOT}/conf/rusk.toml to enable)"
echo ""
