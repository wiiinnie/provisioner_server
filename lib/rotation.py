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

from .config import _log, cfg

SEED_DUSK    = 1000.0
EPOCH_BLOCKS = 2160
STATE_FILE   = os.path.expanduser("~/.sozu_rotation.json")
STATE_CHECK_BLOCKS = 10   # periodic state check cadence

IDLE        = "idle"
ROTATING    = "rotating"
IN_PROGRESS = "in_progress"
ERROR       = "error"

_state_lock   = threading.Lock()
_rot_log      = deque(maxlen=200)
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
}


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


_load_state()


# ── Public API ────────────────────────────────────────────────────────────────

def get_status() -> dict:
    with _state_lock:
        state = dict(_rotation_state)
    with _rot_log_lock:
        log = list(_rot_log)[:50]
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
    threading.Thread(target=_seed_node, args=(target_idx, SEED_DUSK, "deposit fast-path"),
                     daemon=True).start()
    return True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _master_idx() -> int:
    v = cfg("master_idx")
    return int(v) if v is not None else -1


def _rot_indices() -> list[int]:
    from .config import NODE_INDICES
    return [i for i in NODE_INDICES if i != _master_idx()]


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
    return operator_cmd(f"{_NET} {cmd_str}", timeout=timeout, password=_pw())


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

    # ── Retry pending terminate (regular window, once per epoch) ─────────────
    with _state_lock:
        pending_term = _rotation_state.get("pending_terminate")
        pending_deact = _rotation_state.get("pending_deactivate")
    if pending_term and blk_left > rot_win:
        threading.Thread(target=_retry_terminate, args=(pending_term,), daemon=True).start()
        return
    if pending_deact and blk_left > rot_win:
        threading.Thread(target=_retry_deactivate, args=(pending_deact,), daemon=True).start()
        return

    # ── Periodic state check every STATE_CHECK_BLOCKS ────────────────────────
    with _state_lock:
        last_check = _rotation_state["last_state_check_block"]
    if block_height - last_check >= STATE_CHECK_BLOCKS and blk_left > rot_win:
        with _state_lock:
            _rotation_state["last_state_check_block"] = block_height
        threading.Thread(target=_run_state_check,
                         args=(block_height, cur_epoch, blk_left), daemon=True).start()
        return

    # ── Pre-rotation check 5 blocks before window ─────────────────────────────
    with _state_lock:
        precheck_ep = _rotation_state["precheck_done_epoch"]
    if blk_left == rot_win + 5 and cur_epoch != precheck_ep:
        threading.Thread(target=_precheck_rotation,
                         args=(cur_epoch,), daemon=True).start()
        return

    # ── Rotation window entry ─────────────────────────────────────────────────
    with _state_lock:
        last_rot = _rotation_state["last_rotation_epoch"]
    if in_rot_win and cur_epoch != last_rot:
        _rlog_step(f"rotation window entered: blk_left={blk_left} epoch={cur_epoch}")
        threading.Thread(target=_run_rotation, args=(cur_epoch,), daemon=True).start()


# ── Periodic state check ──────────────────────────────────────────────────────

def _run_state_check(block_height: int, cur_epoch: int, blk_left: int) -> None:
    """
    Runs every STATE_CHECK_BLOCKS. Assesses provisioner state and acts:
    seeds inactive nodes, fixes unstaggered maturing nodes.
    """
    try:
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()

        active   = [i for i in rot if nodes.get(i,{}).get("status") == "active"]
        maturing = [i for i in rot if nodes.get(i,{}).get("status") in ("maturing","seeded")]
        inactive = [i for i in rot if nodes.get(i,{}).get("status") == "inactive"]
        tas      = {i: nodes.get(i,{}).get("ta") for i in rot}

        _rlog_info(f"state check blk={block_height}: A:{len(active)} M:{len(maturing)} "
                   f"I:{len(inactive)} ta={tas}")

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
                                     args=(inactive_rot[0], SEED_DUSK, "stagger fix"),
                                     daemon=True).start()
                    return
                else:
                    _rlog_info(f"prov[{waiting_idx}] ta=1, no inactive node to re-seed — clearing wait")
                    with _state_lock:
                        _rotation_state["waiting_for_ta1_idx"] = None
            else:
                _rlog_info(f"waiting for prov[{waiting_idx}] ta=1 (currently ta={waiting_ta})")
                return  # still waiting, don't act on other things

        # ── A:1 M:1 ta=1 → healthy ────────────────────────────────────────────
        if len(active) == 1 and len(maturing) == 1:
            slave_idx = maturing[0]
            slave_ta  = tas.get(slave_idx)
            if slave_ta == 1:
                _rlog_info(f"healthy: rot_active=prov[{active[0]}] rot_slave=prov[{slave_idx}] (ta=1)")
            elif slave_ta == 2:
                _rlog_warn(f"rot_slave prov[{slave_idx}] is ta=2 — not ready for rotation yet")
            return

        # ── A:1 M:0 I:1 → seed inactive ──────────────────────────────────────
        if len(active) == 1 and len(maturing) == 0 and len(inactive) == 1:
            _rlog_warn(f"no rot_slave — seeding inactive prov[{inactive[0]}] with {SEED_DUSK:.0f} DUSK")
            threading.Thread(target=_seed_node,
                             args=(inactive[0], SEED_DUSK, "periodic check"),
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
                _rlog_info(f"both maturing, staggered correctly (ta={ta_vals}) — waiting for epoch transition")
                return
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
                             args=(inactive[0], SEED_DUSK, "periodic check"),
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
                             args=(target, SEED_DUSK, "initial bootstrap"),
                             daemon=True).start()
            return

        _rlog_warn(f"unhandled state: A:{len(active)} M:{len(maturing)} I:{len(inactive)} ta={tas}")

    except Exception as e:
        _rlog_err(f"state check error: {e}")


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
            active_ta  = nodes.get(rot_active_idx,{}).get("ta")
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

def _seed_node(idx: int, amount_dusk: float, reason: str) -> None:
    """Stake exactly amount_dusk into an inactive provisioner."""
    _set_state(IN_PROGRESS)
    try:
        from .wallet import WALLET_PATH
        addr = cfg(f"prov_{idx}_address") or ""
        if not addr:
            _rlog_err(f"seed: prov_{idx}_address not configured")
            _set_state(ROTATING); return

        pool = _pool_dusk()
        if pool < amount_dusk:
            _rlog_warn(f"seed prov[{idx}] ({reason}): pool {pool:.2f} < {amount_dusk:.0f} DUSK — skipping")
            _set_state(ROTATING); return

        amount_lux = int(amount_dusk * 1e9)
        _rlog_step(f"seeding prov[{idx}] {amount_dusk:.0f} DUSK [{reason}]")
        r = _cmd(
            f"pool stake-activate --skip-confirmation "
            f"--amount {amount_lux} --provisioner {addr} "
            f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if r.get("ok"):
            _rlog_ok(f"prov[{idx}] seeded {amount_dusk:.0f} DUSK → maturing (ta=2)")
        else:
            err = r.get("stderr","")[:120]
            _rlog_err(f"seed prov[{idx}] failed: {err}")
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


# ── Main rotation sequence ────────────────────────────────────────────────────

def _run_rotation(cur_epoch: int) -> None:
    _set_state(IN_PROGRESS)
    try:
        from .wallet import WALLET_PATH
        from .rues import register_tx_confirm

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
            _rlog_warn("rotation: no rot_slave (ta=1) — skipping (pre-check should have caught this)")
            _set_state(ROTATING); _bump_epoch(cur_epoch); return

        rot_active_addr = _addr(rot_active_idx, nodes)
        rot_slave_addr  = _addr(rot_slave_idx, nodes)
        stake_dusk  = nodes[rot_active_idx].get("stake_dusk", 0.0)
        locked_dusk = nodes[rot_active_idx].get("locked_dusk", 0.0)
        reward_dusk = nodes[rot_active_idx].get("reward_dusk", 0.0)

        _rlog_step(f"─── rotation epoch {cur_epoch} ───")
        _rlog_info(f"rot_active = prov[{rot_active_idx}] | "
                   f"stake={stake_dusk:.2f} locked={locked_dusk:.2f} reward={reward_dusk:.4f} DUSK")
        _rlog_info(f"rot_slave  = prov[{rot_slave_idx}] (ta=1, no slash penalty)")

        # ── Step 1: liquidate ─────────────────────────────────────────────────
        _rlog_step(f"[1/4] liquidate prov[{rot_active_idx}]")
        liq_evt = register_tx_confirm("liquidate")
        r_liq   = _cmd(f"pool liquidate --skip-confirmation --provisioner {rot_active_addr}")
        if not r_liq.get("ok"):
            err = r_liq.get("stderr","")[:120]
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
        freed_est = max(0.0, stake_dusk - locked_dusk)
        _rlog_ok(f"[1/4] liquidate confirmed — ~{freed_est:.2f} DUSK freed to pool")

        # ── Step 2: terminate ─────────────────────────────────────────────────
        _rlog_step(f"[2/4] terminate prov[{rot_active_idx}] (free rewards)")
        term_evt = register_tx_confirm("terminate")
        r_term   = _cmd(f"pool terminate --skip-confirmation --provisioner {rot_active_addr}")
        if not r_term.get("ok"):
            err = r_term.get("stderr","")[:120]
            _rlog_err(f"[2/4] terminate FAILED: {err} — marking for retry next epoch")
            with _state_lock:
                _rotation_state["pending_terminate"] = rot_active_addr
            _save_state()
        else:
            _rlog_info(f"[2/4] terminate tx sent — waiting for tx/executed confirmation (120s)…")
            if term_evt.wait(timeout=120):
                _rlog_ok(f"[2/4] terminate confirmed — {reward_dusk:.4f} DUSK rewards freed to pool")
                with _state_lock:
                    _rotation_state["pending_terminate"] = None
                _save_state()
            else:
                _rlog_err("[2/4] terminate confirmation timeout — marking for retry")
                with _state_lock:
                    _rotation_state["pending_terminate"] = rot_active_addr
                _save_state()

        # ── Step 3: re-seed rot_active → ta=2 ────────────────────────────────
        _rlog_step(f"[3/4] re-seed prov[{rot_active_idx}] with {SEED_DUSK:.0f} DUSK → rot_seeded (ta=2)")
        pool = _pool_dusk()
        _rlog_info(f"[3/4] pool balance: {pool:.4f} DUSK")
        if pool < SEED_DUSK:
            _rlog_err(f"[3/4] pool {pool:.2f} < {SEED_DUSK:.0f} DUSK — cannot re-seed → ERROR")
            _set_state(ERROR); _bump_epoch(cur_epoch); return

        seed_lux = int(SEED_DUSK * 1e9)
        r_seed   = _cmd(
            f"pool stake-activate --skip-confirmation "
            f"--amount {seed_lux} --provisioner {rot_active_addr} "
            f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if not r_seed.get("ok"):
            _rlog_err(f"[3/4] re-seed FAILED: {r_seed.get('stderr','')[:120]} → ERROR")
            _set_state(ERROR); _bump_epoch(cur_epoch); return
        _rlog_ok(f"[3/4] prov[{rot_active_idx}] re-seeded {SEED_DUSK:.0f} DUSK → ta=2")

        # ── Step 4: allocate bulk to rot_slave ────────────────────────────────
        _rlog_step(f"[4/4] allocate freed stake → rot_slave prov[{rot_slave_idx}] (ta=1, no slash)")
        pool_after = _pool_dusk()
        alloc_dusk = max(0.0, pool_after - SEED_DUSK)  # keep seed buffer
        _rlog_info(f"[4/4] pool after re-seed: {pool_after:.4f} DUSK → allocating {alloc_dusk:.4f} DUSK")

        if alloc_dusk < 10.0:
            _rlog_warn(f"[4/4] only {alloc_dusk:.2f} DUSK available — nothing to allocate to rot_slave")
        else:
            alloc_lux = int(alloc_dusk * 1e9)
            r_alloc   = _cmd(
                f"pool stake-activate --skip-confirmation "
                f"--amount {alloc_lux} --provisioner {rot_slave_addr} "
                f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
            if r_alloc.get("ok"):
                _rlog_ok(f"[4/4] {alloc_dusk:.4f} DUSK → prov[{rot_slave_idx}] (ta=1)")
            else:
                _rlog_err(f"[4/4] allocation failed: {r_alloc.get('stderr','')[:80]}")
                _rlog_info("[4/4] remaining stake stays in pool — deposit race will handle it")

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
