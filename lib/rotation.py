"""
rotation.py — Provisioner rotation automation.

Separation of concerns:
  rotation.py — WHEN to act: epoch scheduling, state assessment, seeding logic
  events.py   — HOW to act: deposit race allocation on incoming DUSK

State machine (simplified — no separate bootstrap states):
  IDLE        — automation disabled
  ROTATING    — enabled; periodic state check + rotation window handling
  IN_PROGRESS — rotation sequence or seeding running (blocks deposit race)
  ERROR       — manual reset needed

Periodic state check (every 10 blocks):
  A:1  M:1  ta=1          → healthy, rotation will fire at window
  A:1  M:1  ta=2          → warn: rot_slave not ready, skip rotation
  A:1  M:0  I:1           → seed inactive with 1000 DUSK (retry every 10 blocks)
  A:1  M:0  I:0           → both rot nodes active (abnormal), log error — treat
                             lower-stake one as "extra active", rotation handles it
  A:0  M:2  ta=[1,2]      → healthy stagger, no action
  A:0  M:2  ta=[1,1]      → deactivate smaller, wait for other ta=1, re-seed
  A:0  M:2  ta=[2,2]      → deactivate smaller, wait for other ta=1, re-seed
  A:0  M:1  I:1           → seed inactive with 1000 DUSK
  A:0  M:0  I:2           → seed one with 1000, wait for ta=1, seed second with 1000

Rotation window sequence (at rot_win entry, guarded by pre-check):
  1. liquidate(rot_active)            → stake → pool [wait tx/executed]
  2. terminate(rot_active)            → rewards → pool [wait tx/executed]
  3. stake_activate(rot_active, 1000) → rot_seeded (ta=2)
  4. stake_activate(rot_slave, pool)  → bulk freed stake to ta=1 node

Deposit race integration:
  - Deposits ≥ 1000 DUSK arriving between 10-block checks also trigger seeding
    (via events.py calling seed_if_needed() — fast path, no polling wait)
  - Deposit race is paused during IN_PROGRESS (is_rotation_in_progress())
"""

import json
import os
import threading
import time
from collections import deque
from datetime import datetime

from .config import _log, cfg, _ROTATION_LOG_PATH

SEED_DUSK    = 1000.0
EPOCH_BLOCKS = 2160
STATE_FILE   = os.path.expanduser("~/.sozu_rotation.json")
STATE_CHECK_BLOCKS = 10   # periodic state check cadence

IDLE        = "idle"
ROTATING    = "rotating"
IN_PROGRESS = "in_progress"
ERROR       = "error"

_state_lock   = threading.Lock()
_rot_log      = deque(maxlen=1000)
_rot_log_lock = threading.Lock()

_rotation_state: dict = {
    "enabled":                False,
    "state":                  IDLE,
    "status_msg":             "",
    "last_rotation_epoch":    -1,
    "last_state_check_block": -1,
    "precheck_done_epoch":    -1,
    "pending_terminate":      None,   # addr to retry terminate
    "pending_deactivate":     None,   # addr to retry deactivate (maturing node fix)
    "waiting_for_ta1_idx":    None,   # idx: waiting for this node to reach ta=1 before re-seeding
    # sweeper
    "sweeper_candidate_block":  -1,   # block when candidate was recorded
    "snatch_done_epoch":         -1,
    "sweeper_candidate_dusk":  0.0,   # pool balance at candidate block
}

# ── Sweeper event delta tracker ───────────────────────────────────────────────
# Tracks net pool balance change between sweeper checks via RUES events.
# +deposit, +reward  →  pool grows
# -activate (anyone) →  pool shrinks
_sweep_delta: float      = 0.0
_sweep_delta_lock        = threading.Lock()

def sweep_on_event(topic: str, decoded: dict) -> None:
    """
    Tracks net pool balance changes between sweeper checks.
    Pool increases: deposit, reward, liquidate
    Pool decreases: activate
    """
    global _sweep_delta
    try:
        if topic in ("deposit", "reward", "liquidate"):
            amount = int(str(decoded.get("amount") or 0)) / 1e9
            with _sweep_delta_lock:
                _sweep_delta += amount
        elif topic == "activate":
            amount = int(str(decoded.get("amount") or decoded.get("value") or 0)) / 1e9
            with _sweep_delta_lock:
                _sweep_delta -= amount
    except Exception:
        pass


# ── Logging ───────────────────────────────────────────────────────────────────

def _rlog(msg: str, ok: bool | None = None, level: str = "info") -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    prefix = {
        "info":  "  ",
        "warn":  "⚠ ",
        "error": "✖ ",
        "ok":    "✓ ",
        "step":  "→ ",
    }.get(level, "  ")
    entry = {"ts": ts, "msg": f"{prefix}{msg}", "ok": ok, "level": level}
    with _rot_log_lock:
        _rot_log.appendleft(entry)
    _log(f"[rotation] {prefix}{msg}")
    with _state_lock:
        _rotation_state["status_msg"] = msg
    try:
        with open(_ROTATION_LOG_PATH, "a") as _f:
            _f.write(f"{ts}  {prefix}{msg}\n")
    except Exception:
        pass


def _rlog_ok(msg: str)    -> None: _rlog(msg, ok=True,  level="ok")
def _rlog_warn(msg: str)  -> None: _rlog(msg, ok=False, level="warn")
def _rlog_err(msg: str)   -> None: _rlog(msg, ok=False, level="error")
def _rlog_step(msg: str)  -> None: _rlog(msg, ok=None,  level="step")
def _rlog_info(msg: str)  -> None: _rlog(msg, ok=None,  level="info")


def _set_state(state: str) -> None:
    with _state_lock:
        prev = _rotation_state["state"]
        _rotation_state["state"] = state
    if prev != state:
        _rlog_info(f"state: {prev} → {state}")
    _save_state()


# ── Persistence ───────────────────────────────────────────────────────────────

def _save_state() -> None:
    try:
        with _state_lock:
            data = dict(_rotation_state)
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        _log(f"[rotation] save error: {e}")


def _load_log_from_file() -> None:
    """Load last 1000 rotation log lines from disk into the in-memory deque."""
    try:
        if not os.path.exists(_ROTATION_LOG_PATH):
            return
        with open(_ROTATION_LOG_PATH) as f:
            lines = f.readlines()
        for line in reversed(lines[-1000:]):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("  ", 1)
            ts  = parts[0].strip() if len(parts) == 2 else ""
            msg = parts[1] if len(parts) == 2 else line
            level = ("error" if "✖" in msg else ("ok" if "✓" in msg else
                     ("warn" if "⚠" in msg else ("step" if "→" in msg else "info"))))
            with _rot_log_lock:
                _rot_log.append({"ts": ts, "msg": msg, "ok": None, "level": level})
        _log(f"[rotation] loaded {min(len(lines), 1000)} log lines from disk")
    except Exception as e:
        _log(f"[rotation] log load error: {e}")


def _load_state() -> None:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                saved = json.load(f)
            with _state_lock:
                _rotation_state.update(saved)
            _rlog_info(f"loaded persisted state: {saved.get('state')} enabled={saved.get('enabled')}")
    except Exception as e:
        _log(f"[rotation] load error: {e}")


_load_log_from_file()
_load_state()


# ── Public API ────────────────────────────────────────────────────────────────

def get_status() -> dict:
    with _state_lock:
        state = dict(_rotation_state)
    with _rot_log_lock:
        log = list(_rot_log)[:200]
    return {**state, "log": log}


def toggle_enabled(pw: str) -> dict:
    if not pw:
        try:
            from .wallet import get_password
            pw = get_password() or ""
        except Exception:
            pass
    with _state_lock:
        enabled = not _rotation_state["enabled"]
        _rotation_state["enabled"] = enabled
        if enabled and _rotation_state["state"] == IDLE:
            _rotation_state["state"] = ROTATING
    _save_state()
    verb = "enabled" if enabled else "disabled"
    _rlog_ok(f"rotation {verb}") if enabled else _rlog_info(f"rotation {verb}")
    if enabled:
        threading.Thread(target=_detect_and_log_state, daemon=True).start()
    return {"enabled": enabled, "state": _rotation_state["state"]}


def reset_error() -> None:
    with _state_lock:
        _rotation_state["state"] = IDLE
        _rotation_state["pending_terminate"] = None
        _rotation_state["pending_deactivate"] = None
        _rotation_state["waiting_for_ta1_idx"] = None
    _save_state()
    _rlog_info("error cleared — re-enabling rotation")


def is_rotation_in_progress() -> bool:
    with _state_lock:
        return _rotation_state["state"] == IN_PROGRESS


def seed_if_needed(amount_dusk: float, source: str) -> bool:
    """
    Called by events.py when a deposit/reward ≥ 1000 DUSK arrives.
    Seeds an inactive rot node if one exists and no rot_slave is present.
    Returns True if seeding was triggered.
    """
    with _state_lock:
        enabled = _rotation_state["enabled"]
        state   = _rotation_state["state"]
    if not enabled or state not in (ROTATING,):
        return False
    if amount_dusk < SEED_DUSK:
        return False

    st    = _assess()
    nodes = st.get("by_idx", {})
    rot   = _rot_indices()
    rot_inactive = [i for i in rot if nodes.get(i,{}).get("status") == "inactive"]
    rot_slave    = [i for i in rot if nodes.get(i,{}).get("status") in ("maturing","seeded")
                    and nodes.get(i,{}).get("ta") == 1]
    if rot_slave:
        return False  # already have a rot_slave, no seeding needed
    if not rot_inactive:
        return False  # no inactive node to seed

    target_idx = rot_inactive[0]
    _rlog_step(f"deposit fast-path: {amount_dusk:.4f} DUSK arrived, seeding prov[{target_idx}]")
    threading.Thread(target=_seed_node, args=(target_idx, SEED_DUSK, "deposit fast-path", nodes),
                     daemon=True).start()
    return True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _master_idx() -> int:
    v = cfg("master_idx")
    return int(v) if v is not None else -1


def _master_indices() -> list[int]:
    """Return all indices excluded from rotation (masters + standbys)."""
    primary = _master_idx()
    # prov0 = primary master, prov1 = standby master — both excluded from rotation
    # Standbys are any configured index below the first rotation node (idx < 2)
    excluded = set()
    if primary >= 0:
        excluded.add(primary)
    excluded.add(1)  # prov1 always standby — never rotated
    return sorted(excluded)


def _rot_indices() -> list[int]:
    from .config import NODE_INDICES
    excl = set(_master_indices())
    return [i for i in NODE_INDICES if i not in excl]


def _pw() -> str:
    try:
        from .wallet import get_password
        return get_password() or ""
    except Exception:
        return ""


def _pool_dusk() -> float:
    try:
        from .pool import _pool_fetch_real
        return _pool_fetch_real()
    except Exception as e:
        _rlog_err(f"pool fetch error: {e}")
        return 0.0


def _cmd(cmd_str: str, timeout: int = 90) -> dict:
    from .wallet import operator_cmd
    from .config import _NET
    return operator_cmd(f"{cmd_str}", timeout=timeout, password=_pw())


def _assess() -> dict:
    from .assess import _assess_state
    return _assess_state(0, _pw())


def _addr(idx: int, nodes: dict) -> str:
    return cfg(f"prov_{idx}_address") or nodes.get(idx, {}).get("address", "")


def _bump_epoch(epoch: int) -> None:
    with _state_lock:
        _rotation_state["last_rotation_epoch"] = epoch
    _save_state()


# ── State detection (on enable + for logging) ─────────────────────────────────

def _detect_and_log_state() -> None:
    """Run assess and log current state summary. Called on enable."""
    try:
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()
        epoch = st.get("epoch", "?")

        summary = []
        for i in rot:
            n  = nodes.get(i, {})
            ta = n.get("ta")
            st_label = n.get("status", "inactive")
            ta_str = f" ta={ta}" if ta is not None else ""
            stake  = n.get("stake_dusk", 0.0)
            summary.append(f"prov[{i}]={st_label}{ta_str} {stake:.0f}DUSK")
        _rlog_info(f"state at epoch {epoch}: {' | '.join(summary)}")

        # Classify
        active   = [i for i in rot if nodes.get(i,{}).get("status") == "active"]
        maturing = [i for i in rot if nodes.get(i,{}).get("status") in ("maturing","seeded")]
        inactive = [i for i in rot if nodes.get(i,{}).get("status") == "inactive"]
        _rlog_info(f"A:{len(active)} M:{len(maturing)} I:{len(inactive)} — automation active")
    except Exception as e:
        _rlog_err(f"state detection error: {e}")


# ── Main entry point — called from rues.py on block_accepted ─────────────────

def on_block(block_height: int) -> None:
    global _sweep_delta
    _log(f"[rotation] on_block called height={block_height}")

    # ── HEAL hook: per-block heal tick (role swap on epoch boundary) ──────────
    try:
        from .heal import tick_heal
        tick_heal(block_height)
    except Exception as _heal_err:
        _log(f"[rotation] heal tick error: {_heal_err}")

    with _state_lock:
        enabled = _rotation_state["enabled"]
        state   = _rotation_state["state"]

    if not enabled or state in (IDLE, ERROR, IN_PROGRESS):
        return

    blk_left   = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)
    rot_win    = int(cfg("rotation_window") or 41)
    snatch_win = int(cfg("snatch_window")   or 11)
    cur_epoch  = block_height // EPOCH_BLOCKS + 1
    in_rot_win = snatch_win < blk_left <= rot_win

    # ── Retry pending terminate/deactivate (regular window, every 10 blocks) ──
    with _state_lock:
        pending_term  = _rotation_state.get("pending_terminate")
        pending_deact = _rotation_state.get("pending_deactivate")
        last_check    = _rotation_state["last_state_check_block"]

    due = block_height - last_check >= STATE_CHECK_BLOCKS
    if due and blk_left > rot_win:
        if pending_term:
            threading.Thread(target=_retry_terminate, args=(pending_term,), daemon=True).start()
        elif pending_deact:
            threading.Thread(target=_retry_deactivate, args=(pending_deact,), daemon=True).start()

    # ── Periodic state check every STATE_CHECK_BLOCKS ────────────────────────
    if due and blk_left > rot_win:
        with _state_lock:
            _rotation_state["last_state_check_block"] = block_height

        # Snapshot and reset delta atomically before starting any threads
        with _sweep_delta_lock:
            delta_snapshot = _sweep_delta
            _sweep_delta = 0.0

        # ── Sweeper: check if previous candidate is still idle ────────────────
        with _state_lock:
            cand_block = _rotation_state["sweeper_candidate_block"]
            cand_dusk  = _rotation_state["sweeper_candidate_dusk"]
        _sweeper_on = bool(cfg("sweeper_enabled"))
        if _sweeper_on and cand_block > 0 and block_height - cand_block >= STATE_CHECK_BLOCKS:
            threading.Thread(target=_run_sweeper,
                             args=(cand_dusk, delta_snapshot, blk_left), daemon=True).start()
            with _state_lock:
                _rotation_state["sweeper_candidate_block"] = -1
                _rotation_state["sweeper_candidate_dusk"]  = 0.0
        elif not _sweeper_on:
            with _state_lock:
                _rotation_state["sweeper_candidate_block"] = -1
                _rotation_state["sweeper_candidate_dusk"]  = 0.0

        threading.Thread(target=_run_state_check,
                         args=(block_height, cur_epoch, blk_left), daemon=True).start()
        return

    # ── Pre-rotation check 5 blocks before window ─────────────────────────────
    with _state_lock:
        precheck_ep = _rotation_state["precheck_done_epoch"]
    if blk_left == rot_win + 5 and cur_epoch != precheck_ep:
        # Mark immediately before spawning thread — prevents duplicate fires
        # from multiple rapid block_accepted events for the same block
        with _state_lock:
            _rotation_state["precheck_done_epoch"] = cur_epoch
        threading.Thread(target=_precheck_rotation,
                         args=(cur_epoch,), daemon=True).start()
        return

    # ── Rotation window entry ─────────────────────────────────────────────────
    with _state_lock:
        last_rot = _rotation_state["last_rotation_epoch"]
    if in_rot_win and cur_epoch != last_rot:
        with _state_lock:
            _rotation_state["last_rotation_epoch"] = cur_epoch
        _rlog_step(f"rotation window entered: blk_left={blk_left} epoch={cur_epoch}")
        threading.Thread(target=_run_rotation, args=(cur_epoch,), daemon=True).start()

    # ── Snatch window: fire sweeper directly on first block ───────────────────
    # No candidate wait — act immediately on whatever is in the pool right now.
    if blk_left == snatch_win and bool(cfg("sweeper_enabled")):
        with _state_lock:
            snatch_done = _rotation_state.get("snatch_done_epoch", -1)
        if cur_epoch != snatch_done:
            with _state_lock:
                _rotation_state["snatch_done_epoch"] = cur_epoch
            threading.Thread(target=_run_snatch, args=(cur_epoch, blk_left), daemon=True).start()



# ── Snatch window direct allocator ───────────────────────────────────────────

def _run_snatch(cur_epoch: int, blk_left: int) -> None:
    """
    Fires on first block of snatch window. No candidate wait — act immediately.
    Only targets ta=1 (rot_slave) — no slash, active next epoch.
    """
    try:
        min_sweep = float(cfg("min_deposit_dusk") or 100.0)
        pool      = _pool_dusk()
        _rlog_info(f"snatch window: pool={pool:.4f} DUSK")
        if pool < min_sweep:
            _rlog_info(f"snatch window: pool {pool:.4f} < {min_sweep:.0f} DUSK — skipping")
            return

        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()
        master = _master_idx()

        rot_slave_idx = next((i for i in rot
                              if i != master
                              and nodes.get(i,{}).get("status") in ("maturing","seeded")
                              and nodes.get(i,{}).get("ta") == 1), None)
        if rot_slave_idx is None:
            _rlog_info("snatch window: no ta=1 target — skipping")
            return

        target_addr = _addr(rot_slave_idx, nodes)
        if not target_addr:
            _rlog_err(f"snatch window: no address for prov[{rot_slave_idx}]")
            return

        try:
            from .assess import _fetch_capacity_cached, _invalidate_capacity_cache
            cap       = _fetch_capacity_cached(_pw())
            available = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
            if available < min_sweep:
                _rlog_warn(f"snatch window: capacity full — available={available:.2f} DUSK — skipping")
                return
            alloc_dusk = min(pool, available)
            _invalidate_capacity_cache()
        except Exception as _ce:
            alloc_dusk = pool
            _rlog_warn(f"snatch window: capacity check error: {_ce} — using pool balance")

        from .wallet import WALLET_PATH
        alloc_lux = round(alloc_dusk * 1e9)
        _rlog_step(f"snatch window: {alloc_dusk:.4f} DUSK → prov[{rot_slave_idx}] (ta=1, no slash)")
        r = _cmd(f"pool stake-activate --skip-confirmation "
                 f"--amount {alloc_lux} --provisioner {target_addr} "
                 f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if r.get("ok"):
            _rlog_ok(f"snatch window: allocated {alloc_dusk:.4f} DUSK → prov[{rot_slave_idx}]")
            try:
                from .events import mark_sweeper_activation
                mark_sweeper_activation(target_addr)
            except Exception:
                pass
        else:
            _rlog_err(f"snatch window: allocation failed: {r.get('stderr','')[:100]}")
    except Exception as e:
        _rlog_err(f"snatch window error: {e}")

# ── Periodic state check ──────────────────────────────────────────────────────

def _run_state_check(block_height: int, cur_epoch: int, blk_left: int) -> None:
    """
    Runs every STATE_CHECK_BLOCKS. Assesses provisioner state and acts:
    seeds inactive nodes, fixes unstaggered maturing nodes.
    """
    _rlog_info(f"state check blk={block_height} epoch={cur_epoch} blk_left={blk_left}")
    try:
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()

        active   = [i for i in rot if nodes.get(i,{}).get("status") == "active"]
        maturing = [i for i in rot if nodes.get(i,{}).get("status") in ("maturing","seeded")]
        inactive = [i for i in rot if nodes.get(i,{}).get("status") == "inactive"]
        tas      = {i: nodes.get(i,{}).get("ta") for i in rot}
        _rlog_info(f"A:{len(active)} M:{len(maturing)} I:{len(inactive)} ta={tas}")

        # ── Master threshold check ────────────────────────────────────────────
        try:
            from .assess import _fetch_capacity_cached as _fcc
            master_idx = _master_idx()
            if master_idx >= 0:
                # v2 heal: two-threshold model (alert + heal), both applied to
                # target_master. Backward-compat: fall back to old master_threshold_pct.
                cap           = _fcc(_pw())
                active_max    = cap.get("active_maximum", 0.0)
                rot_floor_pct = float(cfg("rotation_floor_pct") or 20.0)
                rotation_floor = active_max * rot_floor_pct / 100.0
                target_master = max(0.0, active_max - rotation_floor)

                alert_pct = float(cfg("master_alert_threshold_pct")
                                  or cfg("master_threshold_pct")
                                  or 70.0)
                heal_pct  = float(cfg("master_heal_threshold_pct")
                                  or cfg("master_threshold_pct")
                                  or 50.0)

                master_node   = nodes.get(master_idx, {})
                master_stake  = master_node.get("stake_dusk", 0.0)

                alert_dusk = target_master * alert_pct / 100.0
                heal_dusk  = target_master * heal_pct  / 100.0

                master_lux = round(master_stake * 1e9)
                alert_lux  = round(alert_dusk   * 1e9)

                if master_lux < alert_lux:
                    _rlog_warn(f"master prov[{master_idx}] stake {master_stake:,.2f} DUSK "
                               f"< alert threshold {alert_dusk:,.2f} DUSK "
                               f"({alert_pct:.0f}% of target_master={target_master:,.0f}) — "
                               f"sending alert")
                    try:
                        from .telegram import alert_master_below_threshold
                        alert_master_below_threshold(
                            master_idx, master_stake, alert_dusk,
                            alert_pct, target_master)
                    except Exception as _tg_err:
                        _rlog_warn(f"telegram alert error: {_tg_err}")
                    # ── HEAL hook: notify heal; heal uses its own (lower) threshold ──
                    try:
                        from .heal import check_threshold_and_trigger
                        from .pool import _fast_alloc_pool
                        check_threshold_and_trigger(
                            master_idx, master_stake, active_max, _fast_alloc_pool())
                    except Exception as _heal_err:
                        _rlog_warn(f"heal trigger error: {_heal_err}")
                else:
                    # Condition resolved — reset cooldown so next crossing alerts again
                    try:
                        from .telegram import reset_alert
                        reset_alert("master_below_threshold")
                    except Exception:
                        pass
        except Exception as _thresh_err:
            _rlog_warn(f"master threshold check error: {_thresh_err}")

        # ── Record sweeper candidate (regular window only, sweeper enabled) ──
        # During rotation window, the pool holds freed liquidation stake — don't touch it.
        _rot_win = int(cfg("rotation_window") or 41)
        if bool(cfg("sweeper_enabled")) and blk_left > _rot_win:
            try:
                pool_now   = _pool_dusk()
                min_sweep  = float(cfg("min_deposit_dusk") or 100.0)
                if pool_now >= min_sweep:
                    with _state_lock:
                        _rotation_state["sweeper_candidate_block"] = block_height
                        _rotation_state["sweeper_candidate_dusk"]  = pool_now
                    _rlog_info(f"sweeper: {pool_now:.4f} DUSK in pool — checking again in {STATE_CHECK_BLOCKS} blocks")
            except Exception:
                pass

        # ── Check if we were waiting for a node to reach ta=1 for re-seed ────
        with _state_lock:
            waiting_idx = _rotation_state.get("waiting_for_ta1_idx")
        if waiting_idx is not None:
            waiting_ta = nodes.get(waiting_idx, {}).get("ta")
            if waiting_ta == 1:
                # Find the inactive one to re-seed
                inactive_rot = [i for i in rot if nodes.get(i,{}).get("status") == "inactive"
                                and i != waiting_idx]
                if inactive_rot:
                    _rlog_step(f"prov[{waiting_idx}] now ta=1 — re-seeding prov[{inactive_rot[0]}]")
                    with _state_lock:
                        _rotation_state["waiting_for_ta1_idx"] = None
                    threading.Thread(target=_seed_node,
                                     args=(inactive_rot[0], SEED_DUSK, "stagger fix", nodes),
                                     daemon=True).start()
                    return
                else:
                    _rlog_info(f"prov[{waiting_idx}] ta=1, no inactive node to re-seed — clearing wait")
                    with _state_lock:
                        _rotation_state["waiting_for_ta1_idx"] = None
            else:
                pass  # waiting for ta=1 — silent, logged when it happens
                return  # still waiting, don't act on other things

        # ── A:1 M:1 ta=1 → healthy ────────────────────────────────────────────
        if len(active) == 1 and len(maturing) == 1:
            slave_idx = maturing[0]
            slave_ta  = tas.get(slave_idx)
            if slave_ta == 1:
                pass  # healthy — no log (fires every 10 blocks, noisy)
            elif slave_ta == 2:
                pass  # ta=2 not ready — pre-check will warn 5 blocks before window
            return

        # ── A:1 M:0 I:1 → seed inactive ──────────────────────────────────────
        if len(active) == 1 and len(maturing) == 0 and len(inactive) == 1:
            _rlog_warn(f"no rot_slave — seeding inactive prov[{inactive[0]}] with {SEED_DUSK:.0f} DUSK")
            threading.Thread(target=_seed_node,
                             args=(inactive[0], SEED_DUSK, "periodic check", nodes),
                             daemon=True).start()
            return

        # ── A:1 M:0 I:0 → both rot nodes active (abnormal) ───────────────────
        if len(active) == 2 and len(maturing) == 0:
            stakes = {i: nodes.get(i,{}).get("stake_dusk", 0.0) for i in active}
            smaller = min(stakes, key=stakes.get)
            _rlog_warn(f"both rot nodes active — abnormal state. "
                       f"rotation will liquidate prov[{smaller}] (smaller stake {stakes[smaller]:.0f} DUSK) "
                       f"at next rotation window")
            return

        # ── A:0 M:2 → check stagger ───────────────────────────────────────────
        if len(active) == 0 and len(maturing) == 2:
            ta_vals = sorted([tas.get(i) for i in maturing if tas.get(i) is not None])
            if ta_vals == [1, 2]:
                return  # healthy stagger — silent
            # Both same ta — bad stagger, deactivate smaller
            stakes = {i: nodes.get(i,{}).get("stake_dusk", 0.0) for i in maturing}
            smaller = min(stakes, key=stakes.get)
            larger  = max(stakes, key=stakes.get)
            _rlog_warn(f"both maturing with ta={ta_vals} — deactivating prov[{smaller}] "
                       f"({stakes[smaller]:.0f} DUSK) to fix stagger")
            _rlog_step(f"will re-seed prov[{smaller}] after prov[{larger}] reaches ta=1")
            with _state_lock:
                _rotation_state["waiting_for_ta1_idx"] = larger
            threading.Thread(target=_deactivate_maturing,
                             args=(smaller, nodes), daemon=True).start()
            return

        # ── A:0 M:1 I:1 → seed inactive ──────────────────────────────────────
        if len(active) == 0 and len(maturing) == 1 and len(inactive) == 1:
            _rlog_warn(f"no active provisioner, one maturing — seeding prov[{inactive[0]}]")
            threading.Thread(target=_seed_node,
                             args=(inactive[0], SEED_DUSK, "periodic check", nodes),
                             daemon=True).start()
            return

        # ── A:0 M:0 I:2 → seed one, wait for ta=1, then seed second ──────────
        if len(active) == 0 and len(maturing) == 0 and len(inactive) == 2:
            target = inactive[0]
            other  = inactive[1]
            pool   = _pool_dusk()
            if pool < SEED_DUSK:
                _rlog_warn(f"A:0 M:0 I:2 — need {SEED_DUSK:.0f} DUSK but pool={pool:.2f}. waiting…")
                return
            _rlog_step(f"A:0 M:0 I:2 — seeding prov[{target}] with {SEED_DUSK:.0f} DUSK")
            _rlog_step(f"will seed prov[{other}] after prov[{target}] reaches ta=1")
            with _state_lock:
                _rotation_state["waiting_for_ta1_idx"] = target
            threading.Thread(target=_seed_node,
                             args=(target, SEED_DUSK, "initial bootstrap", nodes),
                             daemon=True).start()
            return

        _rlog_warn(f"unhandled state: A:{len(active)} M:{len(maturing)} I:{len(inactive)} ta={tas}")

    except Exception as e:
        _rlog_err(f"state check error: {e}")


# ── Sweeper ───────────────────────────────────────────────────────────────────

def _run_sweeper(candidate_dusk: float, delta: float, blk_left: int) -> None:
    """
    Fires 10 blocks after a pool candidate was recorded.
    delta is snapshotted atomically in on_block before this thread starts.
    idle_est = candidate + delta (deposits + rewards + liquidations - activations)
    """
    try:
        rot_win    = int(cfg("rotation_window") or 41)
        snatch_win = int(cfg("snatch_window")   or 11)
        min_sweep  = float(cfg("min_deposit_dusk") or 100.0)

        idle_est = max(0.0, candidate_dusk + delta)

        _rlog_info(f"sweeper: re-check — candidate={candidate_dusk:.4f} delta={delta:+.4f} idle≈{idle_est:.4f} DUSK")

        if idle_est < min_sweep:
            _rlog_info(f"sweeper: idle {idle_est:.4f} < {min_sweep:.0f} DUSK threshold — skipping")
            return

        # Fresh pool fetch to confirm
        pool = _pool_dusk()
        _rlog_info(f"sweeper: real pool={pool:.4f} DUSK")
        if pool < min_sweep:
            _rlog_info(f"sweeper: pool empty or below threshold — skipping")
            return

        # Use the lower of estimate and actual (be conservative)
        alloc_dusk = min(idle_est, pool)
        if alloc_dusk < min_sweep:
            return

        in_snatch_or_rot = blk_left <= rot_win

        # Pick target based on window
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()
        master = _master_idx()

        rot_slave_idx  = next((i for i in rot
                               if i != master
                               and nodes.get(i,{}).get("status") in ("maturing","seeded")
                               and nodes.get(i,{}).get("ta") == 1), None)
        rot_active_idx = next((i for i in rot
                               if i != master
                               and nodes.get(i,{}).get("status") == "active"
                               and nodes.get(i,{}).get("ta") == 0), None)

        if in_snatch_or_rot and rot_slave_idx is not None:
            # Cap to global available capacity (no slash penalty but capacity still applies)
            try:
                from .assess import _fetch_capacity_cached
                cap       = _fetch_capacity_cached(_pw())
                available = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
                if available < min_sweep:
                    _rlog_warn(f"sweeper: capacity full — available={available:.2f} DUSK — skipping")
                    return
                alloc_dusk = min(alloc_dusk, available)
            except Exception as _ce:
                _rlog_warn(f"sweeper: capacity check error: {_ce}")
                return
            target_idx = rot_slave_idx
            label = "rot/snatch → rot_slave (ta=1, no slash)"
        elif rot_active_idx is not None:
            # Check BOTH global capacity AND slash headroom for active top-up
            try:
                from .assess import _fetch_capacity_cached, _max_topup_active
                cap       = _fetch_capacity_cached(_pw())
                # Global capacity: active_current already includes locked
                available = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
                if available < min_sweep:
                    _rlog_warn(f"sweeper: capacity full — available={available:.2f} DUSK — skipping")
                    return
                # Slash headroom: additional constraint for topping up active node
                headroom = _max_topup_active(cap)
                if headroom <= 0:
                    _rlog_warn(f"sweeper: rot_active slash headroom exhausted — skipping {alloc_dusk:.2f} DUSK")
                    try:
                        from .events import _dlog
                        _dlog({"type":"deposit_skipped","step":0,
                               "msg":f"sweeper: rot_active slash headroom exhausted — {alloc_dusk:.4f} DUSK stays in pool",
                               "amount":alloc_dusk,"ok":False})
                    except Exception:
                        pass
                    return
                # Cap to the more restrictive of the two limits
                alloc_dusk = min(alloc_dusk, available, headroom)
            except Exception as e:
                _rlog_warn(f"sweeper: capacity check error: {e}")
                return
            target_idx = rot_active_idx
            label = "regular → rot_active (slash-capped)"
        else:
            return  # no valid target

        target_addr = _addr(target_idx, nodes)
        if not target_addr:
            _rlog_err(f"sweeper: no address for prov[{target_idx}]")
            return

        from .wallet import WALLET_PATH
        alloc_lux = round(alloc_dusk * 1e9)
        _rlog_step(f"sweeper: {alloc_dusk:.4f} DUSK idle → prov[{target_idx}] [{label}]")
        r = _cmd(f"pool stake-activate --skip-confirmation "
                 f"--amount {alloc_lux} --provisioner {target_addr} "
                 f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        # Invalidate capacity cache regardless of result — either we used capacity or tx failed
        try:
            from .assess import _invalidate_capacity_cache
            _invalidate_capacity_cache()
        except Exception:
            pass

        if r.get("ok"):
            _rlog_ok(f"sweeper: allocated {alloc_dusk:.4f} DUSK → prov[{target_idx}]")
            try:
                from .events import mark_sweeper_activation
                mark_sweeper_activation(target_addr)
            except Exception:
                pass
        else:
            _rlog_err(f"sweeper: allocation failed: {r.get('stderr','')[:100]}")

    except Exception as e:
        _rlog_err(f"sweeper error: {e}")


# ── Pre-rotation check ────────────────────────────────────────────────────────

def _precheck_rotation(cur_epoch: int) -> None:
    """5 blocks before rotation window. Go/no-go for rotation this epoch."""
    try:
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()

        rot_active_idx = next((i for i in rot
                               if nodes.get(i,{}).get("status") == "active"
                               and nodes.get(i,{}).get("ta") == 0), None)
        rot_slave_idx  = next((i for i in rot
                               if nodes.get(i,{}).get("status") in ("maturing","seeded")
                               and nodes.get(i,{}).get("ta") == 1), None)

        with _state_lock:
            _rotation_state["precheck_done_epoch"] = cur_epoch
        _save_state()

        if rot_active_idx is None:
            _rlog_warn(f"pre-check epoch {cur_epoch}: no rot_active (ta=0) — skipping rotation")
            _bump_epoch(cur_epoch)
            return

        if rot_slave_idx is None:
            # Check for A:2 — both rot nodes active, no maturing node.
            # Special case: liquidate the smaller one to recover healthy state.
            both_active = [i for i in rot if nodes.get(i,{}).get("status") == "active"]
            if len(both_active) == 2:
                stakes = {i: nodes.get(i,{}).get("stake_dusk", 0.0) for i in both_active}
                smaller_idx = min(stakes, key=stakes.get)
                _rlog_warn(f"pre-check epoch {cur_epoch}: A:2 detected — "
                           f"will liquidate prov[{smaller_idx}] ({stakes[smaller_idx]:.2f} DUSK) "
                           f"to recover healthy state")
                # Allow rotation to proceed — _run_rotation handles A:2 path
                return
            ta_summary = {i: nodes.get(i,{}).get("ta") for i in rot}
            _rlog_warn(f"pre-check epoch {cur_epoch}: no rot_slave (ta=1) found, "
                       f"ta={ta_summary} — skipping rotation this epoch")
            _bump_epoch(cur_epoch)
            return

        rot_active_stake = nodes.get(rot_active_idx,{}).get("stake_dusk", 0.0)
        rot_slave_stake  = nodes.get(rot_slave_idx,{}).get("stake_dusk", 0.0)
        _rlog_ok(f"pre-check epoch {cur_epoch}: ✓ ready — "
                 f"rot_active=prov[{rot_active_idx}] {rot_active_stake:.2f} DUSK | "
                 f"rot_slave=prov[{rot_slave_idx}] {rot_slave_stake:.2f} DUSK (ta=1)")

    except Exception as e:
        _rlog_err(f"pre-check error: {e}")


# ── Seed node ─────────────────────────────────────────────────────────────────

def _seed_node(idx: int, amount_dusk: float, reason: str, nodes: dict = None) -> None:
    """Stake exactly amount_dusk into an inactive provisioner.
    If the node has residual rewards (inactive but reward_dusk > 0),
    runs liquidate + terminate first to fully reset the provisioner record.
    """
    _set_state(IN_PROGRESS)
    try:
        from .wallet import WALLET_PATH
        addr = cfg(f"prov_{idx}_address") or ""
        if not addr:
            _rlog_err(f"seed: prov_{idx}_address not configured")
            _set_state(ROTATING); return

        # Pre-reset: if node has residual rewards, liquidate+terminate to clean state
        reward_dusk = (nodes or {}).get(idx, {}).get("reward_dusk", 0.0) if nodes else 0.0
        if reward_dusk > 0:
            _rlog_step(f"seed prov[{idx}]: residual reward {reward_dusk:.4f} DUSK detected — "
                       f"liquidate+terminate to reset before seeding")
            r_liq = _cmd(f"pool liquidate --skip-confirmation --provisioner {addr}")
            if not r_liq.get("ok"):
                err = r_liq.get("stderr", "")[:300]
                if not any(x in err.lower() for x in ("no stake", "nothing to liquidate", "does not exist")):
                    _rlog_err(f"seed prov[{idx}]: pre-liquidate failed: {err}")
                    _set_state(ROTATING); return
                # Already empty — fine, continue to terminate
            r_term = _cmd(f"pool terminate --skip-confirmation --provisioner {addr}")
            if not r_term.get("ok"):
                _rlog_err(f"seed prov[{idx}]: pre-terminate failed: {r_term.get('stderr','')[:300]}")
                _set_state(ROTATING); return
            _rlog_ok(f"seed prov[{idx}]: provisioner reset — proceeding with seed")

        pool = _pool_dusk()
        if pool < amount_dusk:
            _rlog_warn(f"seed prov[{idx}] ({reason}): pool {pool:.2f} < {amount_dusk:.0f} DUSK — skipping")
            _set_state(ROTATING); return

        # Check global capacity — active_current already includes locked
        try:
            from .assess import _fetch_capacity_cached
            cap       = _fetch_capacity_cached(_pw())
            available = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
            if available < amount_dusk:
                _rlog_warn(f"seed prov[{idx}] ({reason}): capacity full — "
                           f"available={available:.2f} DUSK < {amount_dusk:.0f} DUSK — skipping")
                _set_state(ROTATING); return
        except Exception as _ce:
            _rlog_warn(f"seed prov[{idx}]: capacity check error: {_ce} — proceeding")

        amount_lux = round(amount_dusk * 1e9)
        _rlog_step(f"seeding prov[{idx}] {amount_dusk:.0f} DUSK [{reason}]")
        from .rues import register_tx_confirm as _rtc, get_tx_confirm_result as _gtcr
        _seed_evt = _rtc("stake_activate", addr)
        r = _cmd(
            f"pool stake-activate --skip-confirmation "
            f"--amount {amount_lux} --provisioner {addr} "
            f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if not r.get("ok"):
            err = r.get("stderr","")[:120]
            _rlog_err(f"seed prov[{idx}] failed (CLI): {err}")
        else:
            _rlog_info(f"seed prov[{idx}] tx sent — waiting for on-chain confirmation (120s)…")
            if _seed_evt.wait(timeout=120):
                _seed_res = _gtcr("stake_activate", addr)
                _oc_err   = (_seed_res or {}).get("err")
                if _oc_err:
                    _rlog_err(f"seed prov[{idx}] FAILED on-chain: {str(_oc_err)[:120]}")
                else:
                    _rlog_ok(f"prov[{idx}] seeded {amount_dusk:.0f} DUSK → maturing (ta=2)")
            else:
                _rlog_warn(f"seed prov[{idx}] confirmation timeout — state check will verify")
    except Exception as e:
        _rlog_err(f"seed prov[{idx}] error: {e}")
    finally:
        _set_state(ROTATING)


# ── Deactivate maturing node (stagger fix) ────────────────────────────────────

def _deactivate_maturing(idx: int, nodes: dict) -> None:
    """Deactivate a maturing node (no slash — safe for ta=1 or ta=2)."""
    _set_state(IN_PROGRESS)
    try:
        addr = _addr(idx, nodes)
        if not addr:
            _rlog_err(f"deactivate prov[{idx}]: address not configured")
            _set_state(ROTATING); return

        _rlog_step(f"deactivating maturing prov[{idx}] to fix stagger")
        r = _cmd(f"pool stake-deactivate --skip-confirmation --provisioner {addr}")
        if r.get("ok"):
            _rlog_ok(f"prov[{idx}] deactivated — stake returned to pool")
            with _state_lock:
                _rotation_state["pending_deactivate"] = None
        else:
            err = r.get("stderr","")[:120]
            _rlog_err(f"deactivate prov[{idx}] failed: {err} — will retry")
            with _state_lock:
                _rotation_state["pending_deactivate"] = addr
            _save_state()
    except Exception as e:
        _rlog_err(f"deactivate prov[{idx}] error: {e}")
    finally:
        _set_state(ROTATING)


# ── A:2 recovery sequence ─────────────────────────────────────────────────────

def _run_rotation_a2(cur_epoch: int, smaller_idx: int, nodes: dict) -> None:
    """
    Abnormal A:2 recovery: both rot nodes active, no maturing node.
    Liquidate + terminate the smaller one, re-seed it → back to A:1 M:1 next epoch.
    No bulk allocation (no ta=1 target exists).
    """
    from .wallet import WALLET_PATH
    from .rues import register_tx_confirm, get_tx_confirm_result

    addr        = _addr(smaller_idx, nodes)
    stake_dusk  = nodes[smaller_idx].get("stake_dusk", 0.0)
    locked_dusk = nodes[smaller_idx].get("locked_dusk", 0.0)
    reward_dusk = nodes[smaller_idx].get("reward_dusk", 0.0)

    _rlog_step(f"─── A:2 recovery epoch {cur_epoch} ───")
    _rlog_info(f"liquidating smaller rot node prov[{smaller_idx}] "
               f"stake={stake_dusk:.2f} locked={locked_dusk:.2f} DUSK")

    # ── Step 1: liquidate ─────────────────────────────────────────────────────
    _rlog_step(f"[1/3] liquidate prov[{smaller_idx}]")
    liq_evt = register_tx_confirm("liquidate", addr)
    r_liq   = _cmd(f"pool liquidate --skip-confirmation --provisioner {addr}")
    if not r_liq.get("ok"):
        err = r_liq.get("stderr","")[:300]
        if any(x in err.lower() for x in ("no stake", "does not exist", "nothing to liquidate")):
            _rlog_warn("[1/3] liquidate: already empty — proceeding")
            liq_evt.set()
        else:
            _rlog_err(f"[1/3] liquidate FAILED: {err} — aborting A:2 recovery")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return
    else:
        _rlog_info("[1/3] liquidate tx sent — waiting for confirmation (120s)…")

    if not liq_evt.wait(timeout=120):
        _rlog_err("[1/3] liquidate confirmation timeout — aborting A:2 recovery")
        _set_state(ROTATING); _bump_epoch(cur_epoch); return
    # Verify on-chain err (tx made it to a block but may have reverted)
    _liq_res = get_tx_confirm_result("liquidate", addr)
    _liq_err = (_liq_res or {}).get("err")
    if _liq_err:
        _rlog_err(f"[1/3] liquidate REVERTED on-chain: {str(_liq_err)[:200]} — aborting A:2 recovery")
        _set_state(ROTATING); _bump_epoch(cur_epoch); return
    _rlog_ok(f"[1/3] liquidate confirmed — {stake_dusk + locked_dusk:.4f} DUSK freed to pool")

    # ── Step 2: terminate ─────────────────────────────────────────────────────
    _rlog_step(f"[2/3] terminate prov[{smaller_idx}] (free rewards)")
    term_evt = register_tx_confirm("terminate", addr)
    r_term   = _cmd(f"pool terminate --skip-confirmation --provisioner {addr}")
    if not r_term.get("ok"):
        err = r_term.get("stderr","")[:300]
        _rlog_err(f"[2/3] terminate FAILED: {err} — marking for retry")
        with _state_lock:
            _rotation_state["pending_terminate"] = addr
        _save_state()
    else:
        _rlog_info("[2/3] terminate tx sent — waiting for confirmation (120s)…")
        if term_evt.wait(timeout=120):
            _term_res = get_tx_confirm_result("terminate", addr)
            _term_err = (_term_res or {}).get("err")
            if _term_err:
                _rlog_err(f"[2/3] terminate REVERTED on-chain: {str(_term_err)[:200]} — marking for retry")
                with _state_lock:
                    _rotation_state["pending_terminate"] = addr
                _save_state()
            else:
                _rlog_ok(f"[2/3] terminate confirmed — {reward_dusk:.4f} DUSK rewards freed")
                with _state_lock:
                    _rotation_state["pending_terminate"] = None
                _save_state()
        else:
            _rlog_err("[2/3] terminate confirmation timeout — marking for retry")
            with _state_lock:
                _rotation_state["pending_terminate"] = addr
            _save_state()

    # ── Step 3: re-seed → ta=2 ────────────────────────────────────────────────
    # No bulk allocation — the other active node stays untouched.
    # Freed stake goes into pool; re-seed takes 1000 back out.
    _rlog_step(f"[3/3] re-seed prov[{smaller_idx}] with {SEED_DUSK:.0f} DUSK → ta=2")
    seed_evt = register_tx_confirm("stake_activate", addr)
    seed_lux = round(SEED_DUSK * 1e9)
    r_seed   = _cmd(
        f"pool stake-activate --skip-confirmation "
        f"--amount {seed_lux} --provisioner {addr} "
        f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
    if not r_seed.get("ok"):
        _rlog_err(f"[3/3] re-seed FAILED (CLI): {r_seed.get('stderr','')[:300]}")
        _rlog_info("[3/3] state check will retry seeding from pool on next tick")
        _set_state(ROTATING); _bump_epoch(cur_epoch); return
    _rlog_info("[3/3] re-seed tx sent — waiting for on-chain confirmation (120s)…")
    if not seed_evt.wait(timeout=120):
        _rlog_warn("[3/3] re-seed confirmation timeout — state check will verify")
    else:
        _a2_res = get_tx_confirm_result("stake_activate", addr)
        if (_a2_res or {}).get("err"):
            _rlog_err(f"[3/3] re-seed FAILED on-chain: {str(_a2_res['err'])[:200]}")
            _rlog_info("[3/3] state check will retry seeding from pool on next tick")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return
        _rlog_ok(f"[3/3] prov[{smaller_idx}] re-seeded {SEED_DUSK:.0f} DUSK → ta=2 (confirmed)")

    _rlog_ok(f"─── A:2 recovery epoch {cur_epoch} complete ✓ ───")
    _rlog_info(f"next state: prov[{smaller_idx}] → ta=1 next epoch → healthy A:1 M:1")
    _set_state(ROTATING)
    _bump_epoch(cur_epoch)


# ── Main rotation sequence ────────────────────────────────────────────────────

def _run_rotation(cur_epoch: int) -> None:
    _set_state(IN_PROGRESS)
    try:
        # ── HEAL hook: in epoch N+1, heal takes the rotation window ─────
        try:
            from .heal import should_run_harvest, run_harvest
            if should_run_harvest(cur_epoch):
                _rlog_step(f"heal: running HARVEST in place of regular rotation")
                run_harvest(cur_epoch)
                _set_state(ROTATING)
                _bump_epoch(cur_epoch)
                return
        except Exception as _heal_err:
            _rlog_warn(f"heal harvest hook error: {_heal_err}")

        from .wallet import WALLET_PATH
        from .rues import register_tx_confirm, get_tx_confirm_result

        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()

        rot_active_idx = next((i for i in rot
                               if nodes.get(i,{}).get("status") == "active"
                               and nodes.get(i,{}).get("ta") == 0), None)
        rot_slave_idx  = next((i for i in rot
                               if nodes.get(i,{}).get("status") in ("maturing","seeded")
                               and nodes.get(i,{}).get("ta") == 1), None)

        if rot_active_idx is None:
            _rlog_warn("rotation: no rot_active (ta=0) — skipping (pre-check should have caught this)")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return

        if rot_slave_idx is None:
            # A:2 special case — both rot nodes active, no maturing node.
            both_active = [i for i in rot if nodes.get(i,{}).get("status") == "active"]
            if len(both_active) == 2:
                stakes = {i: nodes.get(i,{}).get("stake_dusk", 0.0) for i in both_active}
                smaller_idx = min(stakes, key=stakes.get)
                _run_rotation_a2(cur_epoch, smaller_idx, nodes)
                return
            _rlog_warn("rotation: no rot_slave (ta=1) and not A:2 — skipping")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return

        rot_active_addr = _addr(rot_active_idx, nodes)
        rot_slave_addr  = _addr(rot_slave_idx, nodes)
        stake_dusk  = nodes[rot_active_idx].get("stake_dusk", 0.0)
        locked_dusk = nodes[rot_active_idx].get("locked_dusk", 0.0)
        reward_dusk = nodes[rot_active_idx].get("reward_dusk", 0.0)

        # Freed amounts computed from assessed state — deterministic, no pool query needed.
        # freed_by_liquidate = stake + locked (locked is returned to pool on liquidation)
        # freed_by_terminate = rewards
        freed_by_liquidate = stake_dusk + locked_dusk
        freed_by_terminate = reward_dusk

        _rlog_step(f"─── rotation epoch {cur_epoch} ───")
        _rlog_info(f"rot_active = prov[{rot_active_idx}] | "
                   f"stake={stake_dusk:.2f} locked={locked_dusk:.2f} reward={reward_dusk:.4f} DUSK")
        _rlog_info(f"rot_slave  = prov[{rot_slave_idx}] (ta=1, no slash penalty)")

        # ── Step 1: liquidate ─────────────────────────────────────────────────
        _rlog_step(f"[1/4] liquidate prov[{rot_active_idx}]")
        liq_evt = register_tx_confirm("liquidate", rot_active_addr)
        r_liq   = _cmd(f"pool liquidate --skip-confirmation --provisioner {rot_active_addr}")
        if not r_liq.get("ok"):
            err = r_liq.get("stderr","")[:300]
            if any(x in err.lower() for x in ("no stake", "does not exist", "nothing to liquidate")):
                _rlog_warn(f"[1/4] liquidate: already empty — proceeding")
                liq_evt.set()
            else:
                _rlog_err(f"[1/4] liquidate FAILED: {err} — aborting rotation")
                _set_state(ROTATING); _bump_epoch(cur_epoch); return
        else:
            _rlog_info(f"[1/4] liquidate tx sent — waiting for tx/executed confirmation (120s)…")

        if not liq_evt.wait(timeout=120):
            _rlog_err("[1/4] liquidate confirmation timeout — aborting rotation")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return
        # Verify on-chain err (tx made it to a block but may have reverted)
        _liq_res = get_tx_confirm_result("liquidate", rot_active_addr)
        _liq_err = (_liq_res or {}).get("err")
        if _liq_err:
            _rlog_err(f"[1/4] liquidate REVERTED on-chain: {str(_liq_err)[:200]} — aborting rotation")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return
        _rlog_ok(f"[1/4] liquidate confirmed — {freed_by_liquidate:.4f} DUSK freed to pool")

        # ── Step 2: terminate ─────────────────────────────────────────────────
        _rlog_step(f"[2/4] terminate prov[{rot_active_idx}] (free rewards)")
        term_evt = register_tx_confirm("terminate", rot_active_addr)
        r_term   = _cmd(f"pool terminate --skip-confirmation --provisioner {rot_active_addr}")
        if not r_term.get("ok"):
            err = r_term.get("stderr","")[:300]
            _rlog_err(f"[2/4] terminate FAILED: {err} — marking for retry next epoch")
            freed_by_terminate = 0.0
            with _state_lock:
                _rotation_state["pending_terminate"] = rot_active_addr
            _save_state()
        else:
            _rlog_info(f"[2/4] terminate tx sent — waiting for tx/executed confirmation (120s)…")
            if term_evt.wait(timeout=120):
                _term_res = get_tx_confirm_result("terminate", rot_active_addr)
                _term_err = (_term_res or {}).get("err")
                if _term_err:
                    _rlog_err(f"[2/4] terminate REVERTED on-chain: {str(_term_err)[:200]} — marking for retry")
                    freed_by_terminate = 0.0
                    with _state_lock:
                        _rotation_state["pending_terminate"] = rot_active_addr
                    _save_state()
                else:
                    _rlog_ok(f"[2/4] terminate confirmed — {reward_dusk:.4f} DUSK rewards freed to pool")
                    with _state_lock:
                        _rotation_state["pending_terminate"] = None
                    _save_state()
            else:
                _rlog_err("[2/4] terminate confirmation timeout — marking for retry")
                freed_by_terminate = 0.0
                with _state_lock:
                    _rotation_state["pending_terminate"] = rot_active_addr
                _save_state()

        # ── Step 3: allocate bulk to rot_slave (ta=1) ─────────────────────────
        # Allocate BEFORE re-seeding so the large amount lands safely first.
        # Cap to available global capacity — active_current already includes locked.
        # No capacity cap here — rotation recycles existing stake, not new external DUSK.
        # active_current after liquidation = active_current before - (stake + locked),
        # and we re-allocate exactly that amount back. Net capacity change = zero.
        # ── HEAL hook: if heal is awaiting its epoch-N standby seed, reserve an
        # extra SEED_DUSK so the heal hook at end of rotation has something to fire.
        heal_reserve = 0.0
        try:
            from .heal import wants_epoch_n_seed
            if wants_epoch_n_seed():
                heal_reserve = SEED_DUSK
        except Exception:
            pass
        alloc_dusk = max(0.0, freed_by_liquidate + freed_by_terminate - SEED_DUSK - heal_reserve)
        _rlog_step(f"[3/4] allocate freed stake → rot_slave prov[{rot_slave_idx}] (ta=1, no slash)")
        _rlog_info(
            f"[3/4] liquidate freed {freed_by_liquidate:.4f} DUSK + "
            f"terminate freed {freed_by_terminate:.4f} DUSK - "
            f"seed {SEED_DUSK:.0f} DUSK"
            + (f" - heal_reserve {heal_reserve:.0f} DUSK" if heal_reserve > 0 else "")
            + f" = {alloc_dusk:.4f} DUSK to allocate")

        if alloc_dusk < 10.0:
            _rlog_warn(f"[3/4] only {alloc_dusk:.2f} DUSK to allocate — nothing sent to rot_slave")
        else:
            alloc_lux = round(alloc_dusk * 1e9)
            alloc_evt = register_tx_confirm("stake_activate", rot_slave_addr)
            r_alloc   = _cmd(
                f"pool stake-activate --skip-confirmation "
                f"--amount {alloc_lux} --provisioner {rot_slave_addr} "
                f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
            if r_alloc.get("ok"):
                _rlog_info(f"[3/4] allocation tx sent — waiting for confirmation (120s)…")
                if alloc_evt.wait(timeout=120):
                    _alloc_res = get_tx_confirm_result("stake_activate", rot_slave_addr)
                    if (_alloc_res or {}).get("err"):
                        _rlog_err(f"[3/4] allocation FAILED on-chain: {str(_alloc_res['err'])[:200]}")
                        _rlog_info("[3/4] remaining stake stays in pool — deposit race will handle it")
                    else:
                        _rlog_ok(f"[3/4] {alloc_dusk:.4f} DUSK → prov[{rot_slave_idx}] (ta=1) confirmed")
                else:
                    _rlog_warn(f"[3/4] allocation confirmation timeout — stake likely landed, continuing")
            else:
                _rlog_err(f"[3/4] allocation failed (CLI): {r_alloc.get('stderr','')[:300]}")
                _rlog_info("[3/4] remaining stake stays in pool — deposit race will handle it")

        # ── Step 4: re-seed rot_active → ta=2 ────────────────────────────────
        _rlog_step(f"[4/4] re-seed prov[{rot_active_idx}] with {SEED_DUSK:.0f} DUSK → rot_seeded (ta=2)")
        seed_evt = register_tx_confirm("stake_activate", rot_active_addr)
        seed_lux = round(SEED_DUSK * 1e9)
        r_seed   = _cmd(
            f"pool stake-activate --skip-confirmation "
            f"--amount {seed_lux} --provisioner {rot_active_addr} "
            f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if not r_seed.get("ok"):
            _rlog_err(f"[4/4] re-seed FAILED (CLI): {r_seed.get('stderr','')[:300]}")
            _rlog_info("[4/4] state check will retry seeding from pool on next tick")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return
        _rlog_info(f"[4/4] re-seed tx sent — waiting for on-chain confirmation (120s)…")
        if not seed_evt.wait(timeout=120):
            _rlog_warn("[4/4] re-seed confirmation timeout — state check will verify")
        else:
            _seed_res = get_tx_confirm_result("stake_activate", rot_active_addr)
            if (_seed_res or {}).get("err"):
                _rlog_err(f"[4/4] re-seed FAILED on-chain: {str(_seed_res['err'])[:200]}")
                _rlog_info("[4/4] state check will retry seeding from pool on next tick")
                _set_state(ROTATING); _bump_epoch(cur_epoch); return
            _rlog_ok(f"[4/4] prov[{rot_active_idx}] re-seeded {SEED_DUSK:.0f} DUSK → ta=2 (confirmed)")

        # ── HEAL hook: in epoch N, append one extra seed for standby master ──
        # After rotation's step 4 confirms, if heal is awaiting the epoch-N seed,
        # fire stake_activate on standby (perform_epoch_n_seed handles its own
        # block-gap wait).
        try:
            from .heal import wants_epoch_n_seed, perform_epoch_n_seed
            if wants_epoch_n_seed():
                _rlog_step("heal: appending epoch-N seed of standby after rotation")
                perform_epoch_n_seed(cur_epoch)
        except Exception as _heal_err:
            _rlog_warn(f"heal epoch-N seed hook error: {_heal_err}")

        _rlog_ok(f"─── rotation epoch {cur_epoch} complete ✓ ───")
        _rlog_info(f"next state: prov[{rot_slave_idx}] → active next epoch | "
                   f"prov[{rot_active_idx}] → ta=1 in 2 epochs")
        _set_state(ROTATING)
        _bump_epoch(cur_epoch)

    except Exception as e:
        _rlog_err(f"rotation sequence error: {e}")
        _set_state(ROTATING)
        _bump_epoch(cur_epoch)


# ── Retry helpers ─────────────────────────────────────────────────────────────

def _retry_terminate(addr: str) -> None:
    _rlog_step(f"retrying pending terminate for {addr[:20]}…")
    r = _cmd(f"pool terminate --skip-confirmation --provisioner {addr}")
    if r.get("ok"):
        _rlog_ok("pending terminate OK — rewards freed")
        with _state_lock:
            _rotation_state["pending_terminate"] = None
        _save_state()
    else:
        _rlog_err(f"terminate still failing: {r.get('stderr','')[:80]}")


def _retry_deactivate(addr: str) -> None:
    _rlog_step(f"retrying pending deactivate for {addr[:20]}…")
    r = _cmd(f"pool stake-deactivate --skip-confirmation --provisioner {addr}")
    if r.get("ok"):
        _rlog_ok("pending deactivate OK")
        with _state_lock:
            _rotation_state["pending_deactivate"] = None
        _save_state()
    else:
        _rlog_err(f"deactivate still failing: {r.get('stderr','')[:80]}")
