# SOZU Provisioner Manager (`provisioner_server`)

Automation + dashboard for operating a fleet of Dusk Network staking
provisioners. A Flask app (served by gunicorn) that watches chain events,
runs the stake **rotation** state machine, handles the **deposit race**, and
now supports manual **stake redistribution**. Operated by Hermes Blockchain
Ventures on testnet and mainnet.

---

## Deploy model (READ FIRST)

- Code is edited **locally** (this repo on the Mac), pushed to GitHub, then
  **`git pull` on each VPS** followed by `sudo systemctl restart sozu-dashboard`.
- Two VPSs, both at `/root/provisioner_server`:
  - `sozu-testnet.hermes-stakepool.de`
  - `sozu-mainnet.hermes-stakepool.de`
- **Testnet-first, always.** Anything that can touch stake gets validated on
  testnet across at least one full cycle before it goes near mainnet.
- Historically changes were shipped as **idempotent patcher scripts** (marker
  comments like `[redistribute]`, backups, anchor-uniqueness checks, syntax
  checks). That pattern still lives in the repo history. Going forward, direct
  edits + git are fine, but keep the testnet-first discipline.
- `main` branch = last known-good, mainnet-deployable. Feature work branches
  off it. **Never merge to main / deploy to mainnet until testnet confirms.**

---

## Repository layout

```
provisioner_server/
├── provisioner_dashboard.html   # single-file dashboard UI (HTML+CSS+JS, ~3600 lines)
├── lib/
│   ├── rotation.py              # rotation state machine; on_block() dispatcher (LARGE, ~1700 lines)
│   ├── heal.py                  # master-heal automation (SLATED FOR REMOVAL — see below)
│   ├── redistribute.py          # manual consolidate-and-hop stake redistribution
│   ├── pool.py                  # SOZU pool contract balance + clamp helper
│   ├── assess.py                # per-node state assessment + operator capacity
│   ├── events.py                # RUES event handling, deposit race
│   ├── config.py                # config load/save, constants, node indices
│   ├── telegram.py              # Telegram alert helpers
│   ├── wallet.py                # wallet path + password access
│   ├── nodes.py                 # remote height, node sync
│   ├── rues.py                  # RUES websocket, tx-confirm registry, block waits
│   └── routes/
│       └── system.py            # dashboard API endpoints (Flask blueprint)
```

---

## Domain model (Dusk staking)

### Nodes and roles
- **4 provisioners**, indices `[0,1,2,3]`.
- `MASTER_PAIR = (0, 1)` and `ROTATION_PAIR = (2, 3)` — **disjoint and
  constant** architectural invariant (config.py). Heal/redistribute own the
  master pair; rotation + deposit-race own the rotation pair. Mixing them
  caused a real "standby-leakage" bug historically — do not blur the split.
- One master-pair node is the active **master** (`_master_idx()` in rotation.py
  auto-corrects the cached hint from on-chain state). The other is the standby
  / landing zone.
- Rotation pair alternates each epoch: one is **rot_active** (ta=0), one is
  **rot_slave** (ta=1).

### Timing / eligibility (`ta` = "time to active", epochs)
- `ta=2` (`seeded`): eligibility ≥ epoch+2. `ta=1` (`maturing`): eligibility ==
  epoch+1. `ta=0` (`active`): eligible now. `inactive`: stake < 1000 DUSK.
- Decrements at each epoch boundary: 2 → 1 → 0.
- **Mainnet epoch = 2160 blocks = 6 hours. Blocktime ~10s.** (`EPOCH_BLOCKS=2160`)
- `blk_left = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)`.
- **Rotation window** = `blk_left` 12–41 (`in_rot_win = snatch_win < blk_left <= rot_win`).
- **Snatch window** = `blk_left` 1–11. Liquidated stake sitting in the pool is
  only grabbable by other operators during the snatch window — so any
  liquidate→re-stake sequence MUST complete before `blk_left` reaches
  `snatch_win`.

### The slash rule (critical for redistribution)
- Topping up a **ta==1 (maturing)** node keeps it ta==1 and it activates at the
  next boundary — **NO slash**. This is the mechanic rotation already relies on
  (rot_slave gets topped up during the window and becomes the next rot_master).
- Topping up an **active (ta==0)** node incurs a **10% slash** (10% of the
  top-up goes to locked collateral).
- **Maturing AND slashed stake both count toward `active_maximum`.** So
  `active_current` is the full committed figure; `active_headroom =
  active_maximum − active_current` is automatically correct for gating seeds.

### Capacity (from `assess._fetch_capacity`, via `substrate capacity --format json`)
- `active_current` / `active_maximum` (DUSK) — obey the max; near-cap on mainnet
  (was ~14.24M/15M, ~763k headroom at last check).
- `locked_current` / `locked_maximum` — the slash collateral cap (was ~52/300k).
- `_max_topup_active(cap)` computes the biggest active-node top-up that stays
  within locked headroom (integer LUX math, contract floors `locked_added =
  topup // 10`).
- **Always query capacity fresh before a stake_activate and clamp.** Also clamp
  to on-chain pool balance via `pool.clamp_to_pool_balance(intended_dusk, ctx)`.

### Unstake / fronting order
- Unstakes/withdrawals hit ONE operator per epoch, drawn from that operator's
  provisioners in **ascending node-index order** (node 0 first).
- `heal.is_unstake_target_this_epoch(cur_epoch) -> bool` (cached per epoch,
  fail-open) tells whether we're the target. The biggest stake ideally sits in
  the lowest-index node to absorb unstakes without disturbing others.

---

## Wallet command primitives (rotation.py `_cmd`)

Issued via the wallet CLI against the local node (`state = http://127.0.0.1:8080`):

```
pool liquidate      --skip-confirmation --provisioner {addr}
pool terminate      --skip-confirmation --provisioner {addr}
pool stake-activate --skip-confirmation --amount {lux} --provisioner {addr} \
                    --provisioner-wallet {WALLET_PATH} --provisioner-password '{pw}'
pool stake-deactivate --skip-confirmation --provisioner {addr}
```

Standard tx pattern (confirm + on-chain revert check):

```python
evt = register_tx_confirm(kind, addr)          # from lib.rues
r   = _cmd("pool ... ")
if not r.get("ok"): ...                          # CLI failure
if not evt.wait(timeout=120): ...                # no tx/executed in time
res = get_tx_confirm_result(kind, addr)
if (res or {}).get("err"): ...                   # reverted on-chain
# n+2 block gap before a dependent tx:
blk = (res or {}).get("block_height", 0)
if blk: wait_for_block(blk + 1, timeout=60)      # from lib.rues
```

- 1 DUSK = 1e9 LUX. Amounts to the CLI are integer LUX.
- `_assess()` returns `{by_idx: {idx: {address,status,ta,stake_dusk,locked_dusk,reward_dusk,...}}}`.
- `_addr(idx, nodes)`, `_pw()`, `_pool_dusk()`, `_master_idx()`, `_rot_indices()`
  are the helpers redistribute reuses.
- Logging: `_rlog_ok/warn/err/step/info` (rotation), `_hlog*` (heal),
  `_rdlog*` (redistribute).

---

## Rotation (lib/rotation.py)

- `on_block(block_height)` is the block-tick dispatcher. It calls `tick_heal`,
  then gates on window position: precheck at `rot_win+5`, rotation at window
  entry (`in_rot_win`), snatch sweeper at `blk_left == snatch_win`, periodic
  state checks every `STATE_CHECK_BLOCKS`.
- `_run_rotation(cur_epoch)` runs the liquidate → (terminate) → activate
  sequence for the rotation pair. It also hosts the heal + redistribute hooks.
- Normal rotation: liquidate rot_active → pool, re-seed it 1k (→ new rot_slave),
  top up rot_slave with pool balance (→ new rot_active/rot_master at target).

---

## Redistribution (lib/redistribute.py) — the current feature

### Goal
`rot_master` accumulates far more stake than the rotation target (e.g. 3.6M when
only 1M is wanted). Every rotation liquidates + re-stakes the whole amount,
earning only the "rotation" role and idling it during the window. Move the
excess onto a master-tier node that never recycles — **without the 10% slash**.

### Consolidate-and-hop (4 nodes, master role alternates)
The master role hops between the two `MASTER_PAIR` nodes. Whichever isn't the
active master is the landing zone for the next redistribution. Recycles forever,
no 5th node.

- `cur_master` = the active master now (touched last, liquidated only after its
  replacement is safe).
- `new_master` = the other master-pair node (inactive → pre-seeded → topped up).

### Two-phase flow
- **Epoch N (rotation window):** normal rotation runs, PLUS pre-seed `new_master`
  with 1000 DUSK → ta=2. (`wants_preseed` / `perform_preseed`, hooked AFTER the
  heal epoch-N seed hook in `_run_rotation`.)
- **Boundary N→N+1:** new_master ta 2 → 1.
- **Epoch N+1 (rotation window, BEFORE snatch):** the consolidate
  (`wants_consolidate` / `perform_consolidate`, hooked at the TOP of
  `_run_rotation` so it finishes before snatch):
  1. confirm new_master is **ta==1** (no-master guard)
  2. liquidate rot_master → pool
  3. liquidate cur_master → pool
  4. top-up new_master (ta==1) with `excess + cur_master stake` (NO slash)
  5. activate rot_slave → rot_master at target
- **Boundary N+1→N+2:** new_master active (huge, new master), cur_master freed,
  rot_master at target. Master hopped.

### Guards (all present)
- **Fronting-target defer:** only pre-seed when new_master isn't exposed to
  fronting while we're the unstake target. Rule: exposed iff `new_idx <=
  cur_master_idx` (`_new_master_exposed_to_fronting`). Uses
  `heal.is_unstake_target_this_epoch`.
- **ta==1 no-master precondition:** never liquidate cur_master until new_master
  is confirmed ta==1. Otherwise abort, master untouched, TG alert.
- **Snatch-window timing:** `perform_consolidate` defers if `blk_left <=
  snatch_win + 10` (not enough room for the tx sequence before snatch).
- **Capacity clamp:** `_stake_activate_clamped` clamps to active headroom + pool
  balance and REFUSES to top up an active node.

### Failure-recovery ladder (highest priority)
If cur_master is liquidated but the new_master top-up fails, don't strand the
master stake in the pool through snatch. `_recover_stranded_stake`:
1. retry new_master (ta==1) up to `TOPUP_RETRIES` — cleanest, active in 1 epoch
2. rot_slave (ta==1) — active in 1 epoch (the natural fallback)
3. any other ta==1 node — active in 1 epoch
4. any non-active node — 2 epochs but safe
"Out of pool before snatch" beats "idle 2 epochs" beats "lost to a competitor".

### State machine + persistence
- States: `IDLE → ARMED → PRESEEDED → (reset to IDLE)`.
- State file `~/.sozu_redistribute.json`, log `~/.sozu_redistribute.log`
  (mirrors heal.py conventions). `SEED_DUSK=1000`, `DEFAULT_TARGET_DUSK=1_000_000`,
  `TX_CONFIRM_TIMEOUT=120`, `TOPUP_RETRIES=2`.
- Public API: `arm(target_dusk)`, `get_state()`, `get_status()`, `reset()`.

### API endpoints (routes/system.py, marker `[redistribute]`)
- `GET  /api/redistribute/status`
- `GET  /api/redistribute/preview?target=<dusk>`
- `POST /api/redistribute/arm` (body `{"target_dusk": ...}`)
- `POST /api/redistribute/reset`

### Dashboard UI (provisioner_dashboard.html, marker `[redistribute_ui]`)
- "redistribute stake" button in the actions nav-row.
- `#modal-redistribute` with editable target, live preview (calls `/preview`),
  state line, arm (confirm), reset. JS: `openRedistribute`, `rdPreview`, `rdArm`,
  `rdReset`, `rdRefreshState`.

---

## Heal (lib/heal.py) — SLATED FOR REMOVAL

- Original automated master-replacement: multi-epoch state machine
  (`AWAITING_N → SEEDED → HARVESTING → COMPLETING`), gated on
  `cfg("master_heal_enabled")` at 3 sites (lines ~303/428/814). State file
  `~/.sozu_heal.json`.
- **Currently DISABLED** (`master_heal_enabled: false`) and state reset to idle.
- **It conflicts with redistribution** — both manage the master-pair standby
  node. A heal trigger during a redistribution grabbed prov[1] and is the likely
  cause of the epoch-1743 consolidate skip (see Known issues).
- **Plan: remove heal's action path entirely** once redistribution is confirmed
  working. Keep at most a minimal safety-net (master → zero externally → seed
  reserve 1k + TG alert), no multi-epoch machine.
- Note: `heal_enabled` also appears in config but is **dead** — no source reads
  it (only `master_heal_enabled` gates behavior). The `heal_*` booleans in
  `notification_settings` are alert toggles, not action gates.

---

## Known issues / open work

1. **Consolidate skipped epoch 1743.** Timeline: pre-seed ~epoch 1741 → prov[1]
   ta=2; consolidate checked at 1742 (held, ta=2 correct) and 1744 (reset, ta=0
   too late) but **never fired at 1743** (the ta==1 window). Most likely cause:
   heal triggered concurrently and grabbed prov[1] (heal state showed
   `failed: stake_activate tx/executed timeout`, `standby_idx: 1`). Heal is now
   off; re-test to confirm the consolidate fires with heal disabled. If it still
   skips, there's a redistribute-only timing bug in the hook/window logic.
2. **Single-window brittleness.** Redistribution depends on the consolidate hook
   firing during exactly one epoch (ta==1). If that window is missed for any
   reason, the whole cycle fails and needs manual cleanup (an orphaned 1k active
   seed). Harden: detect the over-mature case and auto-liquidate the stranded
   seed + reset; consider a wider catch than "exactly ta==1".
3. **Heal removal** (see above) — do before trusting redistribution unattended.
4. **Rotation ↔ redistribute window contention.** Both run in the same epoch N+1
   window and share pool balance / capacity. Consolidate is ordered BEFORE the
   normal rotation body; verify on testnet they don't step on each other.
5. **rusk 1.7.1 upgrade (deferred).** The multi-node installer
   (`node_installer_multi.sh`) can't be re-run for rusk 1.7.1: tag scheme changed
   to `dusk-rusk-<ver>`, a `-default` feature suffix is now mandatory, and a new
   `RUSK_CONSENSUS_SPIN_TIME` env var is required (mainnet 1781175600, testnet
   1779886800). Needs an upgrade-only script, not a reinstall.

---

## Infrastructure notes (the fleet)

- Multi-instance nodes: `/opt/dusk{1..4}`, systemd units `rusk-1..rusk-4`, each
  with `RUSK_PROFILE_PATH=/opt/duskN/rusk`. HTTP API on 8080 (ufw only allows
  22, 9001-9004/udp, 7373/tcp externally — 8080 is local-only, which is why the
  wallet can use `127.0.0.1:8080` safely).
- rusk logs to `/var/log/rusk-N.log` (NOT journald — journald only shows systemd
  bookkeeping). Recovery log at `/var/log/rusk-N-recovery.log`.
- **State corruption recovery:** if a node crash-loops with `pointer out of
  bounds` / `Cannot find block header for state root`, its state is corrupt.
  Recover by downloading a fresh snapshot. TESTNET endpoint:
  `https://testnet.nodes.dusk.network/state/list` (latest = last line; use the
  plain number, not the `-archive` variant). The stock `download_state.sh` is
  **mainnet-only and buggy** (`service rusk stop` should be `rusk-N`) — recover
  manually: stop the unit, `rm -rf state chain.db`, `tar -xzf snapshot -C
  /opt/duskN/rusk/` (extract to disk, NOT tmpfs `/tmp`), `chown -R dusk:dusk`,
  start. On-chain stake is unaffected by state resets.
- ext4 mounted `errors=remount-ro,commit=30` — the 30s commit window means an
  unclean host reboot can truncate state mid-write (suspected root cause of a
  4-instance simultaneous corruption event).

---

## Conventions

- **No secrets in git.** Telegram bot token and wallet password live only in
  runtime config on the VPS, never committed. Scan diffs before pushing.
  (`{R._pw()}` and `{WALLET_PATH}` in commands are runtime calls, not literals —
  those are fine.)
- **Testnet-first** for anything touching stake, especially the master.
- Prefer durable, verifiable instructions. When editing large files
  (rotation.py ~1700 lines, dashboard ~3600 lines) work against precise anchors.
- Keep `main` deployable; feature branches for in-test work
  (current: `feature/stake-redistribution`).
- git over SSH (`git@github.com:wiiinnie/provisioner_server.git`).

---

## Good first tasks for Claude Code

- Trace `redistribute.perform_consolidate` / `wants_consolidate` against
  `rotation._run_rotation` and determine why epoch 1743 was skipped (heal
  collision vs. timing bug). Read redistribute.py + rotation.py + heal.py together.
- Design the heal removal: strip the action path, keep (or drop) a minimal
  safety net, ensure nothing else imports the removed symbols.
- Harden redistribute against a missed ta==1 window (issue #2).
