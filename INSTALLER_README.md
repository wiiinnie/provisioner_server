# Multi-Instance Dusk Node Installer (v2)

A hardened multi-instance installer for [Dusk Network](https://dusk.network) provisioner nodes. Lets you run 4 (or more) `rusk` instances on a single VPS, each with its own state, ports, and systemd service — purpose-built for the [SOZU Provisioner Dashboard](https://github.com/wiiinnie/provisioner_server) but useful as a general-purpose tool for anyone running multiple Dusk nodes.

## What it does

- Installs the `rusk` binary at the version pinned for the chosen network
- Configures one rusk instance per `--instance N` invocation, isolated under `/opt/dusk{N}/`
- Writes a systemd unit at `/etc/systemd/system/rusk-{N}.service` with all the env vars rusk actually needs (`RUSK_PROFILE_PATH`, `RUSK_RECOVERY_INPUT`, optional `ExecStartPre` for genesis recovery)
- Configures HTTP on a per-instance port (8080 / 8282 / 8383 / 8484) so the dashboard can monitor each rusk node
- Sets Kadcast public/listen addresses to your auto-detected IPv4 + per-instance UDP port (9000 + N)
- Optionally downloads a state snapshot for fast catchup, or wipes state to regenerate from genesis on first boot

This script is opinionated. It assumes you are running a multi-instance provisioner setup and that you'll use a separate wallet (e.g. `sozu-wallet`) to export consensus keys per instance.

## Quick start

### Testnet, instance 1 (regenerates state from genesis)

```bash
sudo bash node_installer_multi_v2.sh \
    --instance 1 \
    --network testnet \
    --regen-state
```

State will regenerate from genesis on first service start (slow — node will sync from peers). Use this for testnet because the testnet snapshot endpoint currently returns 500.

### Mainnet, instance 1 (downloads latest snapshot for fast catchup)

```bash
sudo bash node_installer_multi_v2.sh \
    --instance 1 \
    --network mainnet \
    --download-state
```

The mainnet snapshot endpoint at `https://nodes.dusk.network/state/list` is healthy. State is downloaded and extracted, so the node starts close to chain tip. Use this for **fresh installs**.

### Mainnet upgrade — preserve existing state (recommended for upgrades)

```bash
sudo bash node_installer_multi_v2.sh \
    --instance 1 \
    --network mainnet
```

Without `--download-state` or `--regen-state`, **existing state is left untouched**. The installer just refreshes the binary, unit file, and configs. When the service starts, rusk loads the existing chain.db and syncs the few blocks it missed during the upgrade — typically seconds. This is the right path for upgrading running mainnet nodes.

### Additional instances (2/3/4) — same network, skip binary re-download

```bash
for i in 2 3 4; do
  sudo bash node_installer_multi_v2.sh \
      --instance $i \
      --network mainnet \
      --download-state \
      --skip-binary-download
done
```

`--skip-binary-download` reuses the rusk binary already cached on the system, much faster than re-fetching the tarball each time.

## After install — required manual steps

The script prints these as numbered "NEXT STEPS" but here they are for reference:

1. **Set the consensus keys password (once, shared across all instances):**

   ```bash
   sudo nano /opt/dusk/services/dusk.conf
   # uncomment and set: DUSK_CONSENSUS_KEYS_PASS=your_password
   ```

2. **Export consensus keys per instance** (instance N uses profile-idx N-1):

   ```bash
   sozu-wallet -w ~/sozu_provisioner --password 'YOUR_PW' \
       export --profile-idx 0 --dir /opt/dusk1/conf --name consensus
   ```

3. **Set ownership and permissions on the exported keys:**

   ```bash
   sudo chown root:dusk /opt/dusk1/conf/consensus.keys
   sudo chmod 640 /opt/dusk1/conf/consensus.keys
   ```

4. **Start the service:**

   ```bash
   sudo systemctl enable rusk-1
   sudo systemctl start rusk-1
   ```

5. **Watch the log:**

   ```bash
   tail -f /var/log/rusk-1.log
   ```

6. **Verify HTTP is reachable:**

   ```bash
   curl http://127.0.0.1:8080/on/node/info -H 'rusk-version: 1.6.0'
   ```

## Options

| Flag | Description |
|---|---|
| `--instance N` | Instance number (default: 1). Creates `/opt/dusk{N}`, service `rusk-{N}`, Kadcast UDP port `9000+N`, HTTP TCP port per the table below. |
| `--network NETWORK` | `mainnet` \| `testnet` \| `devnet` (default: testnet) |
| `--feature FEATURE` | Optional rusk feature variant: `archive` or `prover`. Default uses the standard build. |
| `--download-state` | After install, wipe existing state and download the latest snapshot from the network's nodes endpoint. Fast catchup but only works for networks with a healthy snapshot endpoint (mainnet at the time of writing). Use for **fresh installs**. |
| `--regen-state` | After install, wipe existing state. First service start regenerates state from genesis via `ExecStartPre`. Slow (must sync chain history from peers) but always works. Use for **fresh installs on testnet** (snapshot endpoint is broken). |
| *neither flag* | **Default** — existing state and chain.db are left untouched. The service picks up where it left off and syncs whatever blocks it missed. Use for **upgrades on running nodes**. |
| `--skip-binary-download` | Useful when re-running the installer to fix only the unit file/config — doesn't re-fetch binaries. |
| `--public-ip IP` | Override the auto-detected public IPv4 used for `KADCAST_PUBLIC_ADDRESS` / `KADCAST_LISTEN_ADDRESS`. |
| `--rusk-version VER` | Override the per-network pinned rusk version. Default mapping: mainnet=1.6.0, testnet=1.7.0-rc.0, devnet=1.7.0-rc.0. Example: `--rusk-version 1.7.0` |

### Per-instance port mapping

| Instance | Kadcast UDP | HTTP TCP |
|---|---|---|
| 1 | 9001 | 8080 |
| 2 | 9002 | 8282 |
| 3 | 9003 | 8383 |
| 4 | 9004 | 8484 |
| N (≥2) | 9000+N | (80+N)*101 |

The HTTP port pattern (8080 / 8282 / 8383 / 8484) is what the SOZU Provisioner Dashboard reads from. If you change this, also update the `node_N_ws_port` config keys in the dashboard.

### Firewall

Each instance needs its Kadcast UDP port open at the firewall:

```bash
sudo ufw allow 9001/udp   # instance 1
sudo ufw allow 9002/udp   # instance 2
sudo ufw allow 9003/udp   # instance 3
sudo ufw allow 9004/udp   # instance 4
```

The HTTP port (8080/8282/8383/8484) is bound to `0.0.0.0` so the dashboard can reach it. If the dashboard runs on the same VPS, you can keep these closed at the public firewall:

```bash
# Allow only loopback access to HTTP
for port in 8080 8282 8383 8484; do
  sudo ufw deny in to any port $port proto tcp
done
```

## What's new in v2 vs v1

If you've been using the original `node_installer_multi.sh`, here's what changed:

### Critical bug fixes

- **Rusk URL construction was broken in v1** — generated 404'd URLs because of incorrect tag/version logic (`${RELEASE_TAG}-${VERSION}` produced `dusk-rusk-1.6.0-dusk-rusk-1.6.0/...`). v2 mirrors the official installer's URL pattern: `dusk-rusk-{VERSION}/rusk-{SANITIZED_VERSION}-linux-{ARCH}-{VARIANT}.tar.gz`.
- **Per-network rusk version pinning** — v1 always queried `/releases/latest` (which returns the mainnet release). v2 has a per-network version map matching the official installer's `VERSIONS` table:
  - mainnet: rusk 1.6.0
  - testnet: rusk 1.7.0-rc.0
  - devnet: rusk 1.7.0-rc.0
- **HTTP enabled by default on per-instance port** — v1 wrote `listen = false`, which meant the dashboard couldn't reach individual rusk instances at all. v2 enables HTTP on per-instance ports.
- **HTTP port mapping fixed** — v1 used `8080+N` (8081/8082/8083/8084), which conflicted with the dashboard's expected ports. v2 uses 8080/8282/8383/8484.
- **rusk.toml paths rewritten per instance** — the upstream `{network}.toml` config ships with shared `/opt/dusk/` paths under `[chain]`. v1 left these alone, which meant rusk looked for `db_path` and `consensus_keys_path` in the shared location regardless of which instance was running. v2 rewrites them to `/opt/dusk{N}/...`.
- **`RUSK_PROFILE_PATH` and `RUSK_RECOVERY_INPUT` env vars in unit** — without these, rusk uses `~/.dusk/rusk/state` (i.e. `/root/.dusk/rusk/state` since it runs as root), not `/opt/dusk{N}/rusk/state`. v2 sets both.
- **`ExecStartPre` runs `rusk recovery state` defensively** — generates state from genesis on first boot if state is empty. The `-` prefix tolerates failures (e.g. state already exists).
- **`EnvironmentFile=` lines have `-` prefix** — missing files no longer break service startup.
- **Public IP auto-detected via `curl -4`** — forced IPv4 to avoid the bracketless-IPv6 socket parse error from Dusk's Kadcast layer.
- **rusk-wallet download removed** — Sozu uses `sozu-wallet`, and the official `rusk-wallet` is bundled in the rusk tarball anyway (available at `/opt/dusk{N}/bin/rusk-wallet` if you need it).

### New flags

- `--download-state` — fast catchup via snapshot
- `--regen-state` — wipe state for genesis recovery on next start
- `--skip-binary-download` — useful for re-running on existing installs
- `--public-ip` — manual public IP override
- `--rusk-version` — manual rusk version override

### Defensive checks

- Tarball validation via `file` before extracting — catches HTML 404 pages instead of silently failing at `tar -xzf`
- Refuses to write IPv6 in `KADCAST_PUBLIC_ADDRESS` (sanity-checks for `:` in detected IP)
- Unit content verification after write — re-writes once if expected lines aren't present, fails loudly if even that doesn't work
- Loud warning at end of install if `consensus.keys` is missing

## Troubleshooting

### Service fails to start with "Failed to load environment files"

Almost always means an `EnvironmentFile=` line in the unit refers to a file that doesn't exist, AND lacks the `-` prefix. v2 always uses `-` prefix, so this should not happen with v2. If you see it, check that the unit file was actually written with v2 content:

```bash
sudo grep "EnvironmentFile" /etc/systemd/system/rusk-1.service
# Should show:
#   EnvironmentFile=-/opt/dusk/services/dusk.conf
#   EnvironmentFile=-/opt/dusk1/services/rusk.conf.user
```

### Service fails to start with "Cannot instantiate VM IO Error"

Means rusk can't find its state files. Verify:

```bash
ls -la /opt/dusk1/rusk/state/   # must exist with files
sudo grep "RUSK_PROFILE_PATH" /etc/systemd/system/rusk-1.service
# Should show RUSK_PROFILE_PATH=/opt/dusk1/rusk
```

If `RUSK_PROFILE_PATH` is not set, rusk uses `~/.dusk/rusk/state` and won't find the per-instance state. Re-run the installer.

### Service fails with "consensus.keys ... No such file or directory"

The consensus keys haven't been exported yet. Run step 2 from the post-install steps above.

### Service fails with "invalid socket address syntax"

`KADCAST_PUBLIC_ADDRESS` contains an IPv6 address without brackets. v2 forces IPv4 detection — if it still happens, pass `--public-ip` explicitly with an IPv4.

### Rusk keeps reporting block heights from the wrong network

You have leftover state/chain.db from a previous (different-network) install. The chain.db's stored network identity overrides the rusk.toml config. Wipe and reinstall:

```bash
sudo systemctl stop rusk-1
sudo rm -rf /opt/dusk1/rusk/state /opt/dusk1/rusk/chain.db
sudo bash node_installer_multi_v2.sh --instance 1 --network testnet --regen-state
```

### Testnet snapshot endpoint returns 500

`https://testnet.nodes.dusk.network/state/list` is broken at the time of writing. Use `--regen-state` instead of `--download-state` for testnet — first start will regenerate genesis and sync from peers (slow but reliable).

## License

Based on dusk-network/node-installer (Apache 2.0). Adaptations and multi-instance logic published under the same terms.
