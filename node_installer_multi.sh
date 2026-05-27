#!/usr/bin/env bash
set -e

#############################################
# Multi-Instance Dusk Node Installer (v3)
#
# Fixes from v2 (compatibility with rusk 1.7.0-rc.1 / installer 0.5.19):
#   A. VERSIONS bumped: mainnet 1.6.0 → 1.6.1, testnet 1.7.0-rc.0 → 1.7.0-rc.1.
#   B. Genesis fetch bug: the v2 fallback URL was {network}_genesis.toml which
#      404s — the upstream filename is {network}.genesis (dot, not underscore).
#      v3 copies it from the already-extracted installer tarball instead of
#      hitting the network a second time.
#   C. RUSK_CONSENSUS_SPIN_TIME=1779886800 is now injected into the systemd
#      unit when --network testnet. Mirrors the upstream installer behaviour
#      added for the 2026-05-27 13:00 UTC testnet spin.
#
# Fixes from v1 (node_installer_multi.sh):
#   1. NETWORK env var properly threaded — no hardcoded mainnet anywhere.
#   2. HTTP port mapping uses the correct pattern:
#        instance 1 → 8080
#        instance 2 → 8282
#        instance 3 → 8383
#        instance 4 → 8484
#      (instead of 8081/8082/8083/8084 which conflict with port plans).
#   3. HTTP is ENABLED by default, bound to 0.0.0.0 so the dashboard and
#      external clients can reach it (firewall yourself if needed).
#   4. systemd unit includes RUSK_PROFILE_PATH and RUSK_RECOVERY_INPUT so
#      rusk uses /opt/dusk{N}/rusk/state, not ~/.dusk/rusk/state.
#   5. ExecStartPre runs `rusk recovery state` defensively (with `-` prefix)
#      so first-boot generates state from genesis if needed.
#   6. EnvironmentFile entries use `-` prefix — missing files don't break
#      service startup.
#   7. State download uses the network-appropriate URL:
#        mainnet → https://nodes.dusk.network/state/list
#        testnet → https://testnet.nodes.dusk.network/state/list
#   8. Public IP auto-detected for KADCAST_PUBLIC_ADDRESS via curl ifconfig.me
#      (falls back to 0.0.0.0 with warning if detection fails).
#   9. Unit file content verification after write — defensive against any
#      OS-script that re-writes the unit file.
#  10. Optional --download-state flag to wipe existing state and pull fresh
#      snapshot from the network's nodes endpoint.
#  11. Optional --regen-state flag to wipe and regenerate from genesis.
#  12. Per-network rusk version pinning (mirrors dusk-network/node-installer's
#      VERSIONS map). Mainnet uses rusk 1.6.0, testnet uses 1.7.0-rc.0.
#      Override with --rusk-version. URL construction also fixed (was broken
#      in v1 — generated 404'd URLs because of incorrect tag/version logic).
#  13. rusk-wallet download removed entirely — Sozu uses sozu-wallet, the
#      official rusk-wallet is not needed (and was bundled in the rusk
#      tarball anyway).
#############################################

# ── Defaults ──────────────────────────────────────────────────────────────────
NETWORK="testnet"
INSTANCE=1
FEATURE=""
ARCH=$(uname -m)
KADCAST_BASE_PORT=9000
DOWNLOAD_STATE=false
REGEN_STATE=false
SKIP_BINARY_DOWNLOAD=false
PUBLIC_IP=""
RUSK_VERSION_OVERRIDE=""

# ── Color codes ──────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_help() {
    cat << EOF
Multi-Instance Dusk Node Installer (v3)

Usage: $0 [OPTIONS]

OPTIONS:
    --instance N           Instance number (default: 1)
                           Creates /opt/dusk{N}, service rusk-{N}
                           Kadcast port: 9000+N  (1→9001, 2→9002, ...)
                           HTTP port (0.0.0.0):
                             1→8080, 2→8282, 3→8383, 4→8484, N>4→80(80+N)+(80+N)

    --network NETWORK      Network: mainnet | testnet | devnet (default: testnet)

    --feature FEATURE      Optional feature: archive | prover

    --download-state       After install, wipe existing state and download
                           a fresh snapshot from the correct network endpoint.
                           Speeds up node startup vs regenerating from genesis.

    --regen-state          After install, wipe existing state. First service
                           start will regenerate from genesis (slow, but no
                           network dependency).

    --skip-binary-download Useful for re-running the installer to fix only
                           the unit file / config without re-fetching binaries.

    --public-ip IP         Set KADCAST_PUBLIC_ADDRESS explicitly. Otherwise
                           the installer auto-detects via curl ifconfig.me.

    --rusk-version VER     Override the per-network pinned rusk version.
                           Default mapping (mirrors dusk-network/node-installer
                           release 0.5.19, 2026-05-26):
                             mainnet → 1.6.1
                             testnet → 1.7.0-rc.1
                             devnet  → 1.7.0-rc.1
                           Example: --rusk-version 1.7.0-rc.1

    -h, --help             Show this help

EXAMPLES:
    # Fresh testnet install for instance 1, downloading current state
    sudo bash $0 --instance 1 --network testnet --download-state

    # Fix unit file only on existing instance (no binary download)
    sudo bash $0 --instance 2 --network testnet --skip-binary-download

    # Mainnet install, regenerate state from genesis (slow but no network dep)
    sudo bash $0 --instance 1 --network mainnet --regen-state

EOF
    exit 0
}

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --instance)             INSTANCE="$2";    shift 2 ;;
        --network)              NETWORK="$2";     shift 2 ;;
        --feature)              FEATURE="$2";     shift 2 ;;
        --download-state)       DOWNLOAD_STATE=true; shift ;;
        --regen-state)          REGEN_STATE=true; shift ;;
        --skip-binary-download) SKIP_BINARY_DOWNLOAD=true; shift ;;
        --public-ip)            PUBLIC_IP="$2";   shift 2 ;;
        --rusk-version)         RUSK_VERSION_OVERRIDE="$2"; shift 2 ;;
        -h|--help)              show_help ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; show_help ;;
    esac
done

# ── Validate ──────────────────────────────────────────────────────────────────
if ! [[ "$INSTANCE" =~ ^[0-9]+$ ]] || [ "$INSTANCE" -lt 1 ]; then
    echo -e "${RED}Error: --instance must be a positive integer${NC}"; exit 1
fi
if [[ ! "$NETWORK" =~ ^(mainnet|testnet|devnet)$ ]]; then
    echo -e "${RED}Error: --network must be mainnet, testnet, or devnet${NC}"; exit 1
fi
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Run as root (sudo)${NC}"; exit 1
fi
if [ "$DOWNLOAD_STATE" = true ] && [ "$REGEN_STATE" = true ]; then
    echo -e "${RED}Error: --download-state and --regen-state are mutually exclusive${NC}"; exit 1
fi

# ── Per-instance ports ────────────────────────────────────────────────────────
KADCAST_PORT=$((KADCAST_BASE_PORT + INSTANCE))

# HTTP port mapping:
#   instance 1 → 8080
#   instance N (N>=2) → (80+N)*101  e.g. 2→82*101=8282, 3→83*101=8383, 4→84*101=8484
if [ "$INSTANCE" -eq 1 ]; then
    HTTP_PORT=8080
else
    HTTP_PORT=$(( (80 + INSTANCE) * 101 ))
fi

# ── Per-instance paths ────────────────────────────────────────────────────────
DUSK_ROOT="/opt/dusk${INSTANCE}"
SERVICE_NAME="rusk-${INSTANCE}"
LOG_FILE="/var/log/rusk-${INSTANCE}.log"
LOG_FILE_RECOVERY="/var/log/rusk-${INSTANCE}-recovery.log"
SHARED_DUSK_CONF="/opt/dusk/services/dusk.conf"
CURRENT_USER="${SUDO_USER:-$USER}"

# ── Auto-detect public IPv4 if not provided ───────────────────────────────────
# Force IPv4 (-4) — Dusk Kadcast's socket parser doesn't accept bracketless IPv6.
# If you specifically need IPv6, pass --public-ip with the value you want.
if [ -z "$PUBLIC_IP" ]; then
    echo -e "${BLUE}Detecting public IPv4…${NC}"
    PUBLIC_IP=$(curl -4 -sf --max-time 5 https://ifconfig.me 2>/dev/null \
                || curl -4 -sf --max-time 5 https://api.ipify.org 2>/dev/null \
                || curl -4 -sf --max-time 5 https://icanhazip.com 2>/dev/null \
                || true)
    PUBLIC_IP=$(echo "$PUBLIC_IP" | tr -d '\n\r ')
    if [ -z "$PUBLIC_IP" ]; then
        echo -e "${YELLOW}WARNING: Could not auto-detect public IPv4.${NC}"
        echo -e "${YELLOW}         Falling back to 0.0.0.0 (peers won't be able to dial in).${NC}"
        echo -e "${YELLOW}         Pass --public-ip to set explicitly.${NC}"
        PUBLIC_IP="0.0.0.0"
    elif [[ "$PUBLIC_IP" =~ : ]]; then
        # Sanity check: if for some reason a v6 address came through, refuse it
        echo -e "${RED}ERROR: detected address looks like IPv6: $PUBLIC_IP${NC}"
        echo -e "${RED}       Pass --public-ip with an IPv4 address explicitly.${NC}"
        exit 1
    else
        echo "Detected public IPv4: $PUBLIC_IP"
    fi
fi

# ── Banner ────────────────────────────────────────────────────────────────────
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Multi-Instance Dusk Node Installer (v3)${NC}"
echo -e "${GREEN}========================================${NC}"
echo
echo -e "Instance:           ${YELLOW}${INSTANCE}${NC}"
echo -e "Network:            ${YELLOW}${NETWORK}${NC}"
echo -e "Install path:       ${YELLOW}${DUSK_ROOT}${NC}"
echo -e "Service:            ${YELLOW}${SERVICE_NAME}${NC}"
echo -e "Kadcast (UDP):      ${YELLOW}${KADCAST_PORT}${NC}  (public addr ${PUBLIC_IP}:${KADCAST_PORT})"
echo -e "HTTP (TCP):         ${YELLOW}0.0.0.0:${HTTP_PORT}${NC}"
echo -e "State action:       ${YELLOW}$([ "$DOWNLOAD_STATE" = true ] && echo "download from $NETWORK" || ([ "$REGEN_STATE" = true ] && echo "regen from genesis" || echo "leave as-is / generate on first boot"))${NC}"
[ -n "$FEATURE" ] && echo -e "Feature:            ${YELLOW}${FEATURE}${NC}"
echo
read -p "Continue? (y/N) " -n 1 -r; echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then echo "Cancelled."; exit 0; fi

# ── Stop existing service if running ──────────────────────────────────────────
if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
    echo -e "${YELLOW}Stopping existing ${SERVICE_NAME}…${NC}"
    systemctl stop "${SERVICE_NAME}" || true
fi

# ── Directory structure ───────────────────────────────────────────────────────
echo -e "${GREEN}Creating directory structure…${NC}"
mkdir -p "${DUSK_ROOT}/bin" \
         "${DUSK_ROOT}/conf" \
         "${DUSK_ROOT}/services" \
         "${DUSK_ROOT}/installer/os" \
         "${DUSK_ROOT}/rusk"
mkdir -p "$(dirname "$SHARED_DUSK_CONF")"

# ── Detect OS / arch ──────────────────────────────────────────────────────────
echo -e "${GREEN}Detecting system…${NC}"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    distro=$(echo "$ID" | tr '[:upper:]' '[:lower:]')
else
    echo -e "${RED}Error: /etc/os-release missing${NC}"; exit 1
fi
case "$distro" in linuxmint*) distro="ubuntu";; esac

case "$ARCH" in
    x86_64)         ARCH="x64" ;;
    aarch64|arm64)  ARCH="arm64" ;;
    *) echo -e "${RED}Error: unsupported arch $ARCH${NC}"; exit 1 ;;
esac
echo "OS: $distro  arch: $ARCH"

# ── OpenSSL check ─────────────────────────────────────────────────────────────
if ! command -v openssl >/dev/null 2>&1 \
   || [ "$(openssl version | awk '{print $2}' | cut -d. -f1)" -lt 3 ]; then
    echo -e "${RED}Error: OpenSSL 3+ required${NC}"; exit 1
fi

# ── Download installer package (for OS-specific dep installation) ─────────────
if [ "$SKIP_BINARY_DOWNLOAD" = false ]; then
    INSTALLER_URL="https://github.com/dusk-network/node-installer/tarball/main"
    echo -e "${GREEN}Downloading dusk-network/node-installer tarball…${NC}"
    curl -sL "$INSTALLER_URL" -o "${DUSK_ROOT}/installer/installer.tar.gz" \
        || { echo -e "${RED}Failed to download installer${NC}"; exit 1; }
    tar xf "${DUSK_ROOT}/installer/installer.tar.gz" \
        --strip-components 1 --directory "${DUSK_ROOT}/installer"

    # Source OS-specific script just for install_deps
    OS_SCRIPT="${DUSK_ROOT}/installer/os/${distro}.sh"
    if [ -f "$OS_SCRIPT" ]; then
        echo -e "${GREEN}Loading OS-specific config: ${distro}.sh${NC}"
        # Run install_deps in a subshell so any side effects (writing service
        # files etc.) don't pollute our environment. We only want install_deps.
        ( source "$OS_SCRIPT"; type install_deps >/dev/null 2>&1 && install_deps ) \
            || echo -e "${YELLOW}Warning: install_deps had non-zero exit (continuing)${NC}"
    else
        echo -e "${YELLOW}Warning: no OS script for ${distro} — skipping deps${NC}"
    fi

    # ── Create dusk group ─────────────────────────────────────────────────────
    if ! getent group dusk >/dev/null 2>&1; then
        groupadd dusk
    fi
    if ! groups "$CURRENT_USER" | grep -q "\bdusk\b"; then
        usermod -aG dusk "$CURRENT_USER"
        echo -e "${YELLOW}User $CURRENT_USER added to dusk group (may need re-login)${NC}"
    fi

    # ── Per-network rusk version (mirrors dusk-network/node-installer's VERSIONS map) ──
    # Last synced: upstream release 0.5.19 (2026-05-26).
    # Update these when a new dusk release is published.
    # Override at runtime with --rusk-version.
    declare -A NETWORK_RUSK_VERSION=(
        [mainnet]="1.6.1"
        [testnet]="1.7.0-rc.1"
        [devnet]="1.7.0-rc.1"
    )

    if [ -n "$RUSK_VERSION_OVERRIDE" ]; then
        VERSION="$RUSK_VERSION_OVERRIDE"
        echo -e "${GREEN}Using rusk version override: ${VERSION}${NC}"
    else
        VERSION="${NETWORK_RUSK_VERSION[$NETWORK]}"
        if [ -z "$VERSION" ]; then
            echo -e "${RED}No pinned rusk version for network '${NETWORK}'. Pass --rusk-version explicitly.${NC}"
            exit 1
        fi
        echo -e "${GREEN}Using pinned rusk version for ${NETWORK}: ${VERSION}${NC}"
    fi

    # Tag path uses the full version (e.g. "dusk-rusk-1.7.0-rc.0").
    # Filename uses sanitized version with -rc.* stripped (e.g. "1.7.0").
    RELEASE_TAG="dusk-rusk-${VERSION}"
    SANITIZED_VERSION="${VERSION%-rc.*}"

    # Variant: "default" (no feature), or whatever the user passed
    VARIANT="${FEATURE:-default}"
    if [[ ! "$VARIANT" =~ ^(default|archive|prover)$ ]]; then
        echo -e "${RED}Error: --feature must be 'archive' or 'prover' (or omitted for default)${NC}"
        exit 1
    fi

    # ── Download and extract the rusk tarball (contains rusk + rusk-wallet + scripts) ──
    # URL format (mirrors dusk-network/node-installer's logic):
    # https://github.com/dusk-network/rusk/releases/download/{tag}/rusk-{sanitized_version}-linux-{arch}-{variant}.tar.gz
    # NOTE: rusk-wallet is bundled in the same tarball — we don't fetch it separately.
    RUSK_URL="https://github.com/dusk-network/rusk/releases/download/${RELEASE_TAG}/rusk-${SANITIZED_VERSION}-linux-${ARCH}-${VARIANT}.tar.gz"
    echo -e "${GREEN}Downloading rusk bundle…${NC}"
    echo "  URL: $RUSK_URL"
    if ! curl -fL "$RUSK_URL" -o "${DUSK_ROOT}/installer/rusk.tar.gz"; then
        echo -e "${RED}Failed to download rusk from $RUSK_URL${NC}"
        echo "Check that the asset exists at https://github.com/dusk-network/rusk/releases/tag/${RELEASE_TAG}"
        exit 1
    fi
    # Verify it's actually a gzipped tarball (not an HTML 404 page)
    if ! file "${DUSK_ROOT}/installer/rusk.tar.gz" | grep -q "gzip compressed"; then
        echo -e "${RED}Downloaded file is not a gzipped tarball:${NC}"
        file "${DUSK_ROOT}/installer/rusk.tar.gz"
        head -c 500 "${DUSK_ROOT}/installer/rusk.tar.gz"
        exit 1
    fi
    tar xf "${DUSK_ROOT}/installer/rusk.tar.gz" \
        --strip-components 1 --directory "${DUSK_ROOT}/bin" \
        || { echo -e "${RED}Failed to extract rusk tarball${NC}"; exit 1; }
    chmod +x "${DUSK_ROOT}/bin/"*

    # Confirm rusk binary landed
    if [ ! -x "${DUSK_ROOT}/bin/rusk" ]; then
        echo -e "${RED}rusk binary not found at ${DUSK_ROOT}/bin/rusk after extract${NC}"
        ls -la "${DUSK_ROOT}/bin/"
        exit 1
    fi
    echo -e "${GREEN}rusk binary installed at ${DUSK_ROOT}/bin/rusk${NC}"
    ls -la "${DUSK_ROOT}/bin/" | head -10

    # ── Network config (genesis.toml + rusk.toml base) ────────────────────────
    echo -e "${GREEN}Downloading ${NETWORK} configuration…${NC}"
    CONFIG_URL="https://raw.githubusercontent.com/dusk-network/node-installer/main/conf/${NETWORK}.toml"
    curl -sL "$CONFIG_URL" -o "${DUSK_ROOT}/conf/rusk.toml" \
        || { echo -e "${RED}Failed to download ${NETWORK} config${NC}"; exit 1; }

    # genesis.toml — upstream keeps this in the node-installer repo as
    # {network}.genesis (e.g. testnet.genesis). The installer tarball we
    # already extracted to ${DUSK_ROOT}/installer/ contains it; just copy.
    GENESIS_SRC="${DUSK_ROOT}/installer/conf/${NETWORK}.genesis"
    if [ -f "$GENESIS_SRC" ]; then
        cp -f "$GENESIS_SRC" "${DUSK_ROOT}/conf/genesis.toml"
        echo -e "${GREEN}Copied ${NETWORK}.genesis → ${DUSK_ROOT}/conf/genesis.toml${NC}"
    else
        # Network fallback — using the CORRECT upstream filename (dot, not underscore).
        # v2 used "${NETWORK}_genesis.toml" which 404s.
        GENESIS_URL="https://raw.githubusercontent.com/dusk-network/node-installer/main/conf/${NETWORK}.genesis"
        echo -e "${YELLOW}${GENESIS_SRC} missing — falling back to ${GENESIS_URL}${NC}"
        curl -fsL "$GENESIS_URL" -o "${DUSK_ROOT}/conf/genesis.toml" \
            || echo -e "${RED}Warning: failed to fetch genesis.toml — recovery WILL fail on first start${NC}"
    fi
fi  # end SKIP_BINARY_DOWNLOAD guard

# ── Configure rusk.toml: rewrite per-instance paths + HTTP listener ──────────
echo -e "${GREEN}Configuring rusk.toml…${NC}"

# 1. Rewrite [chain] paths from shared /opt/dusk/ → per-instance /opt/dusk{N}/
#    The upstream {network}.toml ships with shared paths assuming single-instance
#    install. For multi-instance we need each rusk to read/write its own dirs.
sed -i \
    -e "s|'/opt/dusk/rusk'|'${DUSK_ROOT}/rusk'|g" \
    -e "s|\"/opt/dusk/rusk\"|\"${DUSK_ROOT}/rusk\"|g" \
    -e "s|'/opt/dusk/conf/consensus.keys'|'${DUSK_ROOT}/conf/consensus.keys'|g" \
    -e "s|\"/opt/dusk/conf/consensus.keys\"|\"${DUSK_ROOT}/conf/consensus.keys\"|g" \
    "${DUSK_ROOT}/conf/rusk.toml"

# Sanity-check the rewrite landed
if ! grep -q "${DUSK_ROOT}/conf/consensus.keys" "${DUSK_ROOT}/conf/rusk.toml"; then
    echo -e "${YELLOW}WARNING: consensus_keys_path not found in rusk.toml after rewrite${NC}"
    echo -e "${YELLOW}         Inspect ${DUSK_ROOT}/conf/rusk.toml manually${NC}"
fi

# 2. Strip any existing [http] section and append our own (enable, port = HTTP_PORT)
TMPCONF=$(mktemp)
awk '
  BEGIN { skip = 0 }
  /^\[http\]/    { skip = 1; next }
  /^\[/          { skip = 0 }
  skip == 1      { next }
  { print }
' "${DUSK_ROOT}/conf/rusk.toml" > "$TMPCONF"
mv "$TMPCONF" "${DUSK_ROOT}/conf/rusk.toml"

cat >> "${DUSK_ROOT}/conf/rusk.toml" << EOF

# HTTP Configuration - Disabled for multi-instance
[http]
listen = true
listen_address = "0.0.0.0:${HTTP_PORT}"
EOF

# ── Per-instance Kadcast config ───────────────────────────────────────────────
echo -e "${GREEN}Configuring Kadcast (instance ${INSTANCE})…${NC}"
cat > "${DUSK_ROOT}/services/rusk.conf.user" << EOF
# Kadcast configuration for instance ${INSTANCE}
KADCAST_PUBLIC_ADDRESS=${PUBLIC_IP}:${KADCAST_PORT}
KADCAST_LISTEN_ADDRESS=${PUBLIC_IP}:${KADCAST_PORT}
EOF

# ── Ensure shared dusk.conf exists (with placeholder if empty) ────────────────
if [ ! -f "$SHARED_DUSK_CONF" ]; then
    echo -e "${GREEN}Creating shared ${SHARED_DUSK_CONF}…${NC}"
    cat > "$SHARED_DUSK_CONF" << 'EOF'
# Shared dusk config — set DUSK_CONSENSUS_KEYS_PASS to the consensus keys password
# (uncomment + set the value below; required for provisioner participation):
#DUSK_CONSENSUS_KEYS_PASS=your_consensus_password_here
EOF
    chmod 640 "$SHARED_DUSK_CONF"
    chown root:dusk "$SHARED_DUSK_CONF" 2>/dev/null || true
fi

# ── Write systemd unit (defensive: write, then verify content) ────────────────
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
echo -e "${GREEN}Writing systemd unit ${UNIT_FILE}…${NC}"

# Testnet consensus spin time. Mirrors upstream node-installer 0.5.19 behavior:
# the upstream installer injects RUSK_CONSENSUS_SPIN_TIME after RUSK_RECOVERY_INPUT
# for testnet only. Required for rusk 1.7.0-rc.1+ on the new testnet (2026-05-27 spin).
# Current value: 1779886800 = 2026-05-27T13:00:00Z. Override with TESTNET_SPIN_TIME env.
TESTNET_CONSENSUS_SPIN_TIME="${TESTNET_SPIN_TIME:-1779886800}"

SPIN_TIME_LINE=""
if [ "$NETWORK" = "testnet" ]; then
    SPIN_TIME_LINE="Environment=\"RUSK_CONSENSUS_SPIN_TIME=${TESTNET_CONSENSUS_SPIN_TIME}\""
    echo -e "${BLUE}Injecting RUSK_CONSENSUS_SPIN_TIME=${TESTNET_CONSENSUS_SPIN_TIME} (testnet)${NC}"
fi

write_unit() {
    cat > "$UNIT_FILE" << EOF
[Unit]
Description=Dusk Rusk Node - Instance ${INSTANCE} (${NETWORK})
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${DUSK_ROOT}

Environment="RUST_BACKTRACE=full"
Environment="NETWORK=${NETWORK}"
Environment="RUSK_PROFILE_PATH=${DUSK_ROOT}/rusk"
Environment="RUSK_RECOVERY_INPUT=${DUSK_ROOT}/conf/genesis.toml"
${SPIN_TIME_LINE}

EnvironmentFile=-${SHARED_DUSK_CONF}
EnvironmentFile=-${DUSK_ROOT}/services/rusk.conf.user

# Generate state from genesis on first boot if state dir is missing/empty.
# The '-' prefix tolerates failures (e.g. state already exists, recovery returns nonzero).
ExecStartPre=-/bin/bash -c '${DUSK_ROOT}/bin/rusk recovery state >> ${LOG_FILE_RECOVERY} 2>&1 || true'

ExecStart=${DUSK_ROOT}/bin/rusk --config ${DUSK_ROOT}/conf/rusk.toml

StandardOutput=append:${LOG_FILE}
StandardError=append:${LOG_FILE}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
}

write_unit

# Defensive verification: confirm the unit has the critical lines.
# If something else (OS script, package post-install, etc.) clobbered it,
# write again. Maximum 2 attempts then fail loudly.
verify_unit() {
    grep -q "RUSK_PROFILE_PATH=${DUSK_ROOT}/rusk"            "$UNIT_FILE" \
    && grep -q "NETWORK=${NETWORK}"                          "$UNIT_FILE" \
    && grep -q "EnvironmentFile=-${SHARED_DUSK_CONF}"        "$UNIT_FILE" \
    && grep -q "ExecStart=${DUSK_ROOT}/bin/rusk"             "$UNIT_FILE" \
    && { [ "$NETWORK" != "testnet" ] \
         || grep -q "RUSK_CONSENSUS_SPIN_TIME=${TESTNET_CONSENSUS_SPIN_TIME}" "$UNIT_FILE"; }
}

if ! verify_unit; then
    echo -e "${YELLOW}Unit file verification failed — rewriting once${NC}"
    write_unit
    if ! verify_unit; then
        echo -e "${RED}ERROR: Unit file content is unexpected after two writes${NC}"
        echo -e "${RED}Inspect ${UNIT_FILE} manually${NC}"
        exit 1
    fi
fi

# ── Permissions ───────────────────────────────────────────────────────────────
chown -R root:dusk "${DUSK_ROOT}"
chmod -R 775       "${DUSK_ROOT}"
chmod 750          "${DUSK_ROOT}/bin/"* 2>/dev/null || true

# Log files
touch "$LOG_FILE" "$LOG_FILE_RECOVERY"
chown root:dusk "$LOG_FILE" "$LOG_FILE_RECOVERY"
chmod 664       "$LOG_FILE" "$LOG_FILE_RECOVERY"

# ── Logrotate ─────────────────────────────────────────────────────────────────
cat > "/etc/logrotate.d/${SERVICE_NAME}" << EOF
${LOG_FILE} {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0664 root dusk
}
${LOG_FILE_RECOVERY} {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0664 root dusk
}
EOF

# ── Reload systemd ────────────────────────────────────────────────────────────
systemctl daemon-reload

# ── State setup ───────────────────────────────────────────────────────────────
state_dir="${DUSK_ROOT}/rusk/state"
chain_db="${DUSK_ROOT}/rusk/chain.db"

if [ "$DOWNLOAD_STATE" = true ]; then
    echo -e "${GREEN}Downloading ${NETWORK} state snapshot…${NC}"

    # Network-appropriate state list URL
    case "$NETWORK" in
        mainnet) STATE_BASE_URL="https://nodes.dusk.network/state" ;;
        testnet) STATE_BASE_URL="https://testnet.nodes.dusk.network/state" ;;
        devnet)  STATE_BASE_URL="https://devnet.nodes.dusk.network/state" ;;
        *) echo -e "${RED}Unknown network for state URL: ${NETWORK}${NC}"; exit 1 ;;
    esac

    LATEST=$(curl -fsL "${STATE_BASE_URL}/list" 2>/dev/null | tail -n 1 || true)
    if [ -z "$LATEST" ]; then
        echo -e "${YELLOW}WARNING: ${STATE_BASE_URL}/list returned nothing${NC}"
        echo -e "${YELLOW}Falling back to genesis recovery on first start.${NC}"
    else
        echo "Latest state: $LATEST"
        STATE_TARBALL="/tmp/state-${NETWORK}-${LATEST}.tar.gz"
        if ! curl -f -so "$STATE_TARBALL" -L "${STATE_BASE_URL}/${LATEST}"; then
            echo -e "${YELLOW}WARNING: state download failed — falling back to genesis recovery${NC}"
        else
            echo "Downloaded $(du -h "$STATE_TARBALL" | cut -f1) of state data"
            # Wipe and extract
            rm -rf "$state_dir" "$chain_db"
            tar -xzf "$STATE_TARBALL" -C "${DUSK_ROOT}/rusk/" \
                || { echo -e "${RED}Failed to extract state tarball${NC}"; exit 1; }
            chown -R root:dusk "${DUSK_ROOT}/rusk"
            chmod -R 775       "${DUSK_ROOT}/rusk"
            echo -e "${GREEN}State extracted from ${NETWORK} snapshot ${LATEST}${NC}"
        fi
    fi
fi

if [ "$REGEN_STATE" = true ]; then
    echo -e "${GREEN}Wiping state — first start will regenerate from genesis${NC}"
    rm -rf "$state_dir" "$chain_db"
fi

# ── Final summary ─────────────────────────────────────────────────────────────
echo
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Installation Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo
echo -e "Instance:      ${YELLOW}${INSTANCE} (${NETWORK})${NC}"
echo -e "Service:       ${YELLOW}${SERVICE_NAME}${NC}"
echo -e "HTTP:          ${YELLOW}0.0.0.0:${HTTP_PORT}${NC}"
echo -e "Kadcast:       ${YELLOW}${PUBLIC_IP}:${KADCAST_PORT}/udp${NC}"
echo -e "Logs:          ${YELLOW}${LOG_FILE}${NC}"
echo -e "Recovery log:  ${YELLOW}${LOG_FILE_RECOVERY}${NC}"
echo
echo -e "${YELLOW}FIREWALL — open Kadcast port:${NC}"
echo "  ufw allow ${KADCAST_PORT}/udp"
echo
echo -e "${YELLOW}NEXT STEPS:${NC}"
echo
echo "1. Set consensus keys password (shared across instances):"
echo "   sudo nano ${SHARED_DUSK_CONF}"
echo "   # uncomment and set: DUSK_CONSENSUS_KEYS_PASS=your_password"
echo
echo "2. Export consensus keys (per instance — uses profile-idx $((INSTANCE-1))):"
echo "   sozu-wallet -w ~/sozu_provisioner --password 'YOUR_PW' \\"
echo "     export --profile-idx $((INSTANCE-1)) --dir ${DUSK_ROOT}/conf --name consensus"
echo "   # OR use dusk-wallet if not using provisioner_server's wallet"
echo
echo "3. Start the service:"
echo "   sudo systemctl enable ${SERVICE_NAME}"
echo "   sudo systemctl start ${SERVICE_NAME}"
echo
echo "4. Watch the log:"
echo "   tail -f ${LOG_FILE}"
echo
echo "5. Verify HTTP is reachable:"
echo "   curl http://127.0.0.1:${HTTP_PORT}/on/node/info -H \"rusk-version: \${VERSION:-1.6.1}\""
echo "   # (or from another host: curl http://${PUBLIC_IP}:${HTTP_PORT}/...)"
echo
if [ "$DOWNLOAD_STATE" = false ] && [ "$REGEN_STATE" = false ]; then
    echo -e "${YELLOW}NOTE: --download-state and --regen-state were not specified.${NC}"
    echo -e "${YELLOW}      First start will run \`rusk recovery state\` (regen from genesis)${NC}"
    echo -e "${YELLOW}      if state is empty. This may take several minutes.${NC}"
    echo
fi

# ── Final consensus.keys sanity check ─────────────────────────────────────────
if [ ! -f "${DUSK_ROOT}/conf/consensus.keys" ]; then
    echo -e "${RED}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║  CONSENSUS KEYS MISSING                                          ║${NC}"
    echo -e "${RED}║                                                                  ║${NC}"
    echo -e "${RED}║  ${DUSK_ROOT}/conf/consensus.keys does not exist.       ║${NC}"
    echo -e "${RED}║  The service WILL fail to start until this file is created.     ║${NC}"
    echo -e "${RED}║                                                                  ║${NC}"
    echo -e "${RED}║  Run step 2 from NEXT STEPS above to export the keys, then       ║${NC}"
    echo -e "${RED}║  start the service.                                              ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo
fi
