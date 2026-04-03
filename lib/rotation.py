"""
rotation.py — Provisioner rotation automation.

events.py   — reactive: incoming pool balance → allocate (deposit race)
rotation.py — proactive: epoch boundary → rotate stake between rot nodes

States:
  IDLE         — disabled
  BOOTSTRAP_A  — seeding first rotation provisioner with pool balance
  BOOTSTRAP_B  — first seeded, waiting for ta=1, then seed second with 1000 DUSK
  ROTATING     — steady state; rotation fires each rotation window
  IN_PROGRESS  — rotation sequence running (blocks deposit race + double-fire)
  ERROR        — manual reset needed

Bootstrap (from two empty provisioners):
  epoch E:    stake_activate(rot_a, pool)     → rot_a ta=2
  epoch E+1:  rot_a ta=1 → stake_activate(rot_b, 1000) → rot_b ta=2
  epoch E+2:  rot_a → active, rot_b → ta=1   → ROTATING

Rotation sequence (on rotation window entry):
  1. liquidate(rot_active)             → stake → pool
  2. terminate(rot_active)             → rewards → pool
  3. stake_activate(rot_active, 1000)  → rot_seeded (ta=2)
  4. stake_activate(rot_slave, pool)   → bulk to ta=1 node (no slash)

Post-rotation:
  - deposit race resumes targeting rot_slave (ta=1, no slash penalty)
  - rot_seeded becomes rot_slave next epoch, rot_slave becomes rot_active

⚠ TODO (remind Matthias):
  - Snatch window rotation behaviour not yet defined
  - SEED_DUSK (1000) not yet configurable in UI
  - Deposit race during IN_PROGRESS is paused — arrivals during ~90s sequence are missed
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

IDLE        = "idle"
BOOTSTRAP_A = "bootstrap_a"
BOOTSTRAP_B = "bootstrap_b"
ROTATING    = "rotating"
IN_PROGRESS = "in_progress"
ERROR       = "error"

_state_lock = threading.Lock()
_rot_log    = deque(maxlen=100)
_rot_log_lock = threading.Lock()

_rotation_state: dict = {
    "enabled":             False,
    "state":               IDLE,
    "status_msg":          "",
    "last_rotation_epoch": -1,
    "pending_terminate":   None,
    "rot_a_idx":           None,
    "rot_b_idx":           None,
    "precheck_done_epoch": -1,   # epoch where precheck was already done
}


# ── Logging ───────────────────────────────────────────────────────────────────

def _rlog(msg: str, ok: bool | None = None) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with _rot_log_lock:
        _rot_log.appendleft({"ts": ts, "msg": msg, "ok": ok})
    _log(f"[rotation] {msg}")
    with _state_lock:
        _rotation_state["status_msg"] = msg


def _set_state(state: str) -> None:
    with _state_lock:
        _rotation_state["state"] = state
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
            _log(f"[rotation] loaded: state={saved.get('state')} enabled={saved.get('enabled')}")
    except Exception as e:
        _log(f"[rotation] load error: {e}")


_load_state()


# ── Public API ────────────────────────────────────────────────────────────────

def get_status() -> dict:
    with _state_lock:
        state = dict(_rotation_state)
    with _rot_log_lock:
        log = list(_rot_log)[:30]
    return {**state, "log": log}


def toggle_enabled(pw: str) -> dict:
    """Enable/disable. Uses server-cached password if pw is empty."""
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
            # Don't call assess here — it's slow; detect on first block
            _rotation_state["state"] = ROTATING  # optimistic; corrected by first assess
    _save_state()
    _rlog(f"rotation {'enabled — will detect state on next block' if enabled else 'disabled'}",
          ok=enabled)
    if enabled:
        # Detect actual state in background so toggle returns fast
        threading.Thread(target=_detect_and_set_state, args=(pw,), daemon=True).start()
    return {"enabled": enabled, "state": _rotation_state["state"]}


def reset_error() -> None:
    with _state_lock:
        _rotation_state["state"] = IDLE
        _rotation_state["pending_terminate"] = None
    _save_state()
    _rlog("error cleared — will re-detect state on next block")


def is_rotation_in_progress() -> bool:
    with _state_lock:
        return _rotation_state["state"] == IN_PROGRESS


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
        return _pool_fetch_real(_pw())
    except Exception as e:
        _rlog(f"pool fetch error: {e}", ok=False)
        return 0.0


def _cmd(cmd_str: str, timeout: int = 90) -> dict:
    from .wallet import operator_cmd
    from .config import _NET
    return operator_cmd(f"{_NET} {cmd_str}", timeout=timeout, password=_pw())


def _assess() -> dict:
    from .assess import _assess_state
    return _assess_state(0, _pw())


# ── State detection ───────────────────────────────────────────────────────────

def _detect_and_set_state(pw: str = "") -> None:
    try:
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()
        states = {i: nodes.get(i, {}).get("status", "inactive") for i in rot}
        tas    = {i: nodes.get(i, {}).get("ta") for i in rot}

        active_n   = sum(1 for s in states.values() if s == "active")
        maturing_n = sum(1 for s in states.values() if s in ("maturing", "seeded"))
        inactive_n = sum(1 for s in states.values() if s == "inactive")

        _rlog(f"state detection: {states} ta={tas}")

        if active_n >= 1 and maturing_n >= 1:
            new_state = ROTATING
        elif active_n == 0 and inactive_n == 2:
            new_state = BOOTSTRAP_A
        elif maturing_n >= 1 and inactive_n >= 1:
            new_state = BOOTSTRAP_B
        elif active_n == 0 and maturing_n == 2:
            new_state = BOOTSTRAP_B
        else:
            new_state = ROTATING

        _rlog(f"detected state → {new_state}", ok=True)
        _set_state(new_state)
    except Exception as e:
        _rlog(f"state detection error: {e}", ok=False)


# ── Main entry point ──────────────────────────────────────────────────────────

def on_block(block_height: int, pw: str = "") -> None:
    with _state_lock:
        enabled = _rotation_state["enabled"]
        state   = _rotation_state["state"]

    if not enabled or state in (ERROR, IN_PROGRESS):
        return

    blk_left   = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)
    rot_win    = int(cfg("rotation_window") or 41)
    snatch_win = int(cfg("snatch_window")   or 11)
    cur_epoch  = block_height // EPOCH_BLOCKS
    in_rot_win = snatch_win < blk_left <= rot_win

    # Retry pending terminate in regular window
    with _state_lock:
        pending = _rotation_state.get("pending_terminate")
    if pending and blk_left > rot_win:
        threading.Thread(target=_retry_terminate, args=(pending,), daemon=True).start()
        return

    if state == BOOTSTRAP_A:
        if blk_left > rot_win:
            threading.Thread(target=_run_bootstrap_a, daemon=True).start()
        return

    if state == BOOTSTRAP_B:
        threading.Thread(target=_run_bootstrap_b, daemon=True).start()
        return

    if state == ROTATING:
        with _state_lock:
            last_epoch  = _rotation_state["last_rotation_epoch"]
            precheck_ep = _rotation_state["precheck_done_epoch"]

        # ── Pre-rotation check: 5 blocks before rotation window ───────────────
        precheck_blk = rot_win + 5
        if blk_left == precheck_blk and cur_epoch != precheck_ep:
            threading.Thread(target=_precheck_rotation,
                             args=(cur_epoch, rot_win, snatch_win), daemon=True).start()
            return

        # ── Fire rotation on rotation window entry ────────────────────────────
        if in_rot_win and cur_epoch != last_epoch:
            _rlog(f"rotation window: blk_left={blk_left} epoch={cur_epoch}")
            threading.Thread(target=_run_rotation, args=(cur_epoch,), daemon=True).start()


# ── Pre-rotation check ────────────────────────────────────────────────────────

def _precheck_rotation(cur_epoch: int, rot_win: int, snatch_win: int) -> None:
    """Assess state 5 blocks before rotation window. Skip with warning if not ready."""
    try:
        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()

        rot_active_idx = next((i for i in rot
                               if nodes.get(i,{}).get("status") == "active"
                               and nodes.get(i,{}).get("ta") == 0), None)
        rot_slave_idx  = next((i for i in rot
                               if nodes.get(i,{}).get("status") == "maturing"
                               and nodes.get(i,{}).get("ta") == 1), None)

        with _state_lock:
            _rotation_state["precheck_done_epoch"] = cur_epoch
        _save_state()

        if rot_active_idx is None:
            _rlog(f"⚠ pre-check epoch {cur_epoch}: no rot_active (ta=0) — skipping rotation", ok=False)
            with _state_lock:
                _rotation_state["last_rotation_epoch"] = cur_epoch
            _save_state()
            return

        if rot_slave_idx is None:
            ta_map = {i: nodes.get(i,{}).get("ta") for i in rot}
            _rlog(f"⚠ pre-check epoch {cur_epoch}: no rot_slave (ta=1) found, ta={ta_map} "
                  f"— skipping rotation this epoch", ok=False)
            with _state_lock:
                _rotation_state["last_rotation_epoch"] = cur_epoch
            _save_state()
            return

        _rlog(f"✓ pre-check epoch {cur_epoch}: rot_active=prov[{rot_active_idx}] "
              f"rot_slave=prov[{rot_slave_idx}] — ready to rotate", ok=True)

    except Exception as e:
        _rlog(f"pre-check error: {e}", ok=False)


# ── Bootstrap A ───────────────────────────────────────────────────────────────

def _run_bootstrap_a() -> None:
    _set_state(IN_PROGRESS)
    try:
        from .wallet import WALLET_PATH
        rot   = _rot_indices()
        st    = _assess()
        nodes = st.get("by_idx", {})

        rot_a_idx = next((i for i in rot if nodes.get(i,{}).get("status") == "inactive"), None)
        if rot_a_idx is None:
            _rlog("BOOTSTRAP_A: no inactive rotation node — re-detecting state")
            _detect_and_set_state(); return

        addr = cfg(f"prov_{rot_a_idx}_address") or ""
        if not addr:
            _rlog(f"BOOTSTRAP_A: prov_{rot_a_idx}_address not configured", ok=False)
            _set_state(ERROR); return

        pool = _pool_dusk()
        if pool < SEED_DUSK:
            _rlog(f"BOOTSTRAP_A: pool {pool:.2f} < {SEED_DUSK} DUSK — waiting", ok=False)
            _set_state(BOOTSTRAP_A); return

        amount_lux = int(pool * 1e9)
        _rlog(f"BOOTSTRAP_A: seeding prov[{rot_a_idx}] with {pool:.4f} DUSK")
        r = _cmd(f"pool stake-activate --skip-confirmation "
                 f"--amount {amount_lux} --provisioner {addr} "
                 f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if not r.get("ok"):
            _rlog(f"BOOTSTRAP_A failed: {r.get('stderr','')[:120]}", ok=False)
            _set_state(BOOTSTRAP_A); return

        with _state_lock:
            _rotation_state["rot_a_idx"] = rot_a_idx
        _rlog(f"BOOTSTRAP_A: prov[{rot_a_idx}] seeded → waiting for ta=1", ok=True)
        _set_state(BOOTSTRAP_B)
    except Exception as e:
        _rlog(f"BOOTSTRAP_A error: {e}", ok=False)
        _set_state(BOOTSTRAP_A)


# ── Bootstrap B ───────────────────────────────────────────────────────────────

def _run_bootstrap_b() -> None:
    try:
        rot   = _rot_indices()
        st    = _assess()
        nodes = st.get("by_idx", {})

        with _state_lock:
            rot_a_idx = _rotation_state.get("rot_a_idx")

        if rot_a_idx is None:
            rot_a_idx = next((i for i in rot
                              if nodes.get(i,{}).get("status") in ("maturing","seeded")), None)

        if rot_a_idx is None:
            _rlog("BOOTSTRAP_B: cannot find rot_a — restarting")
            _set_state(BOOTSTRAP_A); return

        rot_a = nodes.get(rot_a_idx, {})

        # rot_a active → seed rot_b if not done
        if rot_a.get("status") == "active":
            rot_b_idx = next((i for i in rot if i != rot_a_idx), None)
            rot_b     = nodes.get(rot_b_idx, {}) if rot_b_idx is not None else {}
            if rot_b.get("status") in ("maturing","seeded","active"):
                _rlog("BOOTSTRAP_B: complete → ROTATING", ok=True)
                _set_state(ROTATING); return
            _run_seed_rot_b(rot_b_idx); return

        ta = rot_a.get("ta")
        if ta != 1:
            _rlog(f"BOOTSTRAP_B: rot_a[{rot_a_idx}] ta={ta} — waiting for ta=1")
            return

        rot_b_idx = next((i for i in rot if i != rot_a_idx), None)
        if rot_b_idx is None:
            _rlog("BOOTSTRAP_B: no rot_b slot", ok=False)
            _set_state(ERROR); return
        if nodes.get(rot_b_idx,{}).get("status") in ("maturing","seeded","active"):
            _rlog("BOOTSTRAP_B: rot_b already seeded → ROTATING", ok=True)
            _set_state(ROTATING); return

        _run_seed_rot_b(rot_b_idx)
    except Exception as e:
        _rlog(f"BOOTSTRAP_B error: {e}", ok=False)


def _run_seed_rot_b(rot_b_idx: int) -> None:
    _set_state(IN_PROGRESS)
    from .wallet import WALLET_PATH
    addr = cfg(f"prov_{rot_b_idx}_address") or ""
    if not addr:
        _rlog(f"prov_{rot_b_idx}_address not configured", ok=False)
        _set_state(ERROR); return

    amount_lux = int(SEED_DUSK * 1e9)
    _rlog(f"BOOTSTRAP_B: seeding prov[{rot_b_idx}] with {SEED_DUSK:.0f} DUSK")
    r = _cmd(f"pool stake-activate --skip-confirmation "
             f"--amount {amount_lux} --provisioner {addr} "
             f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
    if not r.get("ok"):
        _rlog(f"BOOTSTRAP_B seed failed: {r.get('stderr','')[:120]}", ok=False)
        _set_state(BOOTSTRAP_B); return

    with _state_lock:
        _rotation_state["rot_b_idx"] = rot_b_idx
    _rlog(f"BOOTSTRAP_B: prov[{rot_b_idx}] seeded → ROTATING", ok=True)
    _set_state(ROTATING)


# ── Main rotation sequence ────────────────────────────────────────────────────

def _run_rotation(cur_epoch: int) -> None:
    _set_state(IN_PROGRESS)
    try:
        from .wallet import WALLET_PATH

        st    = _assess()
        nodes = st.get("by_idx", {})
        rot   = _rot_indices()

        rot_active_idx = next((i for i in rot
                               if nodes.get(i,{}).get("status") == "active"
                               and nodes.get(i,{}).get("ta") == 0), None)
        rot_slave_idx  = next((i for i in rot
                               if nodes.get(i,{}).get("status") == "maturing"
                               and nodes.get(i,{}).get("ta") == 1), None)

        # Guards (pre-check may have already caught these, but double-check)
        if rot_active_idx is None:
            _rlog("ROTATION: no rot_active — skipping", ok=False)
            _set_state(ROTATING); _bump_epoch(cur_epoch); return

        if rot_slave_idx is None:
            _rlog("ROTATION: no rot_slave (ta=1) — skipping (pre-check should have caught this)", ok=False)
            _set_state(ROTATING); _bump_epoch(cur_epoch); return

        rot_active_addr = cfg(f"prov_{rot_active_idx}_address") or nodes[rot_active_idx].get("address","")
        rot_slave_addr  = cfg(f"prov_{rot_slave_idx}_address")  or nodes[rot_slave_idx].get("address","")
        stake_dusk      = nodes[rot_active_idx].get("stake_dusk", 0.0)
        locked_dusk     = nodes[rot_active_idx].get("locked_dusk", 0.0)
        reward_dusk     = nodes[rot_active_idx].get("reward_dusk", 0.0)
        freed_est       = max(0.0, stake_dusk - locked_dusk) + reward_dusk

        _rlog(f"ROTATION epoch {cur_epoch}: prov[{rot_active_idx}] "
              f"stake={stake_dusk:.2f} locked={locked_dusk:.2f} reward={reward_dusk:.4f} "
              f"→ est. {stake_dusk - locked_dusk + reward_dusk:.2f} DUSK to free")

        # ── Step 1: liquidate — wait for tx/executed confirmation ─────────────
        from .rues import register_tx_confirm, get_tx_confirm_result
        liq_evt = register_tx_confirm("liquidate")
        r_liq   = _cmd(f"pool liquidate --skip-confirmation --provisioner {rot_active_addr}")
        if not r_liq.get("ok"):
            err = r_liq.get("stderr","")[:120]
            if any(x in err.lower() for x in ("no stake", "does not exist", "nothing to liquidate")):
                _rlog("liquidate: already empty — proceeding", ok=True)
                liq_evt.set()  # skip wait
            else:
                _rlog(f"liquidate FAILED: {err}", ok=False)
                _set_state(ROTATING); _bump_epoch(cur_epoch); return
        else:
            _rlog("liquidate tx sent — waiting for on-chain confirmation…")

        confirmed = liq_evt.wait(timeout=120)
        if not confirmed:
            _rlog("liquidate confirmation timeout — aborting rotation", ok=False)
            _set_state(ROTATING); _bump_epoch(cur_epoch); return
        _rlog("liquidate confirmed on-chain ✓", ok=True)

        # ── Step 2: terminate — wait for tx/executed confirmation ─────────────
        term_evt = register_tx_confirm("terminate")
        r_term   = _cmd(f"pool terminate --skip-confirmation --provisioner {rot_active_addr}")
        if not r_term.get("ok"):
            err = r_term.get("stderr","")[:120]
            _rlog(f"terminate FAILED: {err} — will retry next epoch", ok=False)
            with _state_lock:
                _rotation_state["pending_terminate"] = rot_active_addr
            _save_state()
            # Continue without terminate — stake is freed, proceed with re-seed
        else:
            _rlog("terminate tx sent — waiting for on-chain confirmation…")
            confirmed_term = term_evt.wait(timeout=120)
            if confirmed_term:
                _rlog("terminate confirmed on-chain ✓ — rewards freed to pool", ok=True)
                with _state_lock:
                    _rotation_state["pending_terminate"] = None
                _save_state()
            else:
                _rlog("terminate confirmation timeout — marking for retry", ok=False)
                with _state_lock:
                    _rotation_state["pending_terminate"] = rot_active_addr
                _save_state()

        # ── Step 3: re-seed rot_active → rot_seeded (ta=2) ───────────────────
        pool = _pool_dusk()
        if pool < SEED_DUSK:
            _rlog(f"pool {pool:.2f} DUSK < {SEED_DUSK} — cannot re-seed", ok=False)
            _set_state(ERROR); _bump_epoch(cur_epoch); return

        seed_lux = int(SEED_DUSK * 1e9)
        r_seed = _cmd(f"pool stake-activate --skip-confirmation "
                      f"--amount {seed_lux} --provisioner {rot_active_addr} "
                      f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
        if not r_seed.get("ok"):
            _rlog(f"re-seed FAILED: {r_seed.get('stderr','')[:120]}", ok=False)
            _set_state(ERROR); _bump_epoch(cur_epoch); return
        _rlog(f"re-seed prov[{rot_active_idx}] {SEED_DUSK:.0f} DUSK → rot_seeded (ta=2)", ok=True)

        # ── Step 4: allocate bulk to rot_slave (ta=1, no slash penalty) ───────
        time.sleep(2)
        pool_after = _pool_dusk()
        alloc_dusk = max(0.0, pool_after - SEED_DUSK)  # leave seed buffer

        if alloc_dusk < 10.0:
            _rlog(f"pool after re-seed: {pool_after:.2f} DUSK — nothing to allocate to rot_slave")
        else:
            alloc_lux = int(alloc_dusk * 1e9)
            r_alloc = _cmd(f"pool stake-activate --skip-confirmation "
                           f"--amount {alloc_lux} --provisioner {rot_slave_addr} "
                           f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
            if r_alloc.get("ok"):
                _rlog(f"allocated {alloc_dusk:.4f} DUSK → rot_slave prov[{rot_slave_idx}] (ta=1, no slash)", ok=True)
            else:
                _rlog(f"rot_slave alloc failed: {r_alloc.get('stderr','')[:80]} "
                      f"— deposit race will handle remainder", ok=False)

        _rlog(f"rotation epoch {cur_epoch} complete ✓", ok=True)
        _set_state(ROTATING)
        _bump_epoch(cur_epoch)

    except Exception as e:
        _rlog(f"rotation sequence error: {e}", ok=False)
        _set_state(ROTATING)
        _bump_epoch(cur_epoch)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bump_epoch(epoch: int) -> None:
    with _state_lock:
        _rotation_state["last_rotation_epoch"] = epoch
    _save_state()


def _retry_terminate(addr: str) -> None:
    _rlog(f"retrying pending terminate for {addr[:16]}…")
    r = _cmd(f"pool terminate --skip-confirmation --provisioner {addr}")
    if r.get("ok"):
        _rlog("pending terminate OK", ok=True)
        with _state_lock:
            _rotation_state["pending_terminate"] = None
        _save_state()
    else:
        _rlog(f"terminate still failing: {r.get('stderr','')[:80]}", ok=False)
