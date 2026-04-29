"""
heal.py — Master provisioner automated healing (v2).

Simpler architecture than v1: heal coordinates with rotation's normal window
work instead of running its own seeding. Two moments matter:

  Epoch N   (threshold tripped):
    Normal rotation runs as usual (liquidate rot_active, terminate, alloc to
    rot_slave, reseed rot_active with 1k). We append ONE extra stake_activate
    that seeds the standby master with 1k. Standby begins the maturing path.

  Epoch N+1 (standby now ta=1, ready to become next master):
    Instead of normal rotation, heal takes the rotation window and runs the
    harvest sequence:
      1. liquidate active master        (→ pool/stake freed; locked slashed)
      2. terminate active master        (→ rewards freed)
      3. liquidate active rot_active    (→ stake freed)
      4. stake_activate standby_master  (bulk freed stake, capped to target)
      5. stake_activate new rot_slave   (1k, old rot_active now reseeded)

  Epoch N+2 boundary:
    Standby (now topped up) becomes ACTIVE naturally. Heal updates
    cfg["master_idx"]. Old master is now standby (inactive). Done.

Transaction serialization:
  - One tx in flight at a time (no batching).
  - After each tx confirms, wait until block_accepted >= executed_block + 1
    before firing the next (ensures 2-block gap for liquidate→terminate edge
    case and is applied uniformly for safety).

State machine:
  IDLE        — no heal in progress.
  AWAITING_N  — threshold tripped; waiting for next rotation window to seed
                standby (via rotation hook, not by heal).
  SEEDED      — standby has been stake_activated with 1k in epoch N; waiting
                for ta=1 (happens at start of epoch N+1).
  HARVESTING  — in rotation window of epoch N+1; harvest sequence running.
  COMPLETING  — harvest done; waiting for epoch N+2 boundary to swap roles.
  FAILED      — harvest aborted (tx reverted or timeout); resets to IDLE
                at next threshold check. No retry counter.

Role model (hardcoded: master pair is {0, 1}, rotation pair is {2, 3}):
  master_idx   — cfg key; current active master
  standby_idx  — the other slot in {0, 1}
"""

import json
import os
import threading
from collections import deque
from datetime import datetime

from .config import _log, cfg


# ── Constants ─────────────────────────────────────────────────────────────────
SEED_DUSK       = 1000.0
EPOCH_BLOCKS    = 2160
BLOCK_GAP       = 1               # wait for block_accepted >= executed + GAP
                                  #   before firing next tx. 1 means earliest
                                  #   inclusion is executed + 2.
STATE_FILE      = os.path.expanduser("~/.sozu_heal.json")
HEAL_LOG_PATH   = os.path.expanduser("~/.sozu_heal.log")

# Master pair — hardcoded. rotation.py excludes these from rot_indices.
MASTER_PAIR     = (0, 1)

# ── State constants ───────────────────────────────────────────────────────────
IDLE        = "idle"
AWAITING_N  = "awaiting_n"      # threshold tripped; waiting for rotation window to seed
SEEDED      = "seeded"          # standby has 1k; waiting for ta=1
HARVESTING  = "harvesting"      # harvest sequence running
COMPLETING  = "completing"      # harvest done; awaiting epoch N+2 boundary
FAILED      = "failed"          # harvest step failed; reset on next threshold check

# ── Internal state ────────────────────────────────────────────────────────────
_heal_state_lock = threading.Lock()
_heal_log        = deque(maxlen=500)
_heal_log_lock   = threading.Lock()

_heal_state: dict = {
    "state":            IDLE,
    "old_master_idx":   None,
    "standby_idx":      None,
    "triggered_block":  None,
    "status_msg":       "",
    "deferral_count":   0,              # number of consecutive harvest deferrals
                                        # due to unstake-target gate. Force-run at
                                        # max_harvest_deferrals (default 3).
    "last_deferred_epoch": None,        # epoch at which we last deferred
}


# ── Unstake-target check (cache scoped to current epoch) ──────────────────────
# The unstake target is the operator with the HIGHEST max_eligibility AMONG
# operators with active_current > 0. Heal should defer harvest by one epoch
# if we are the target, to avoid liquidating while unstakes are hitting us.
# Bounded at max_harvest_deferrals (default 3) to prevent infinite deferral.

_unstake_target_cache: dict = {"epoch": None, "is_us": None, "reason": ""}
_unstake_cache_lock = threading.Lock()


def _fetch_operators(pw: str) -> list:
    """Return list of registered operator BLS addresses from substrate."""
    from .rotation import _cmd
    from .config   import _NET
    r = _cmd(f"substrate operators --format json", timeout=30)
    if not r.get("ok"):
        raise RuntimeError(f"substrate operators failed: {r.get('stderr','')[:200]}")
    raw = (r.get("stdout") or "").strip()
    if not raw:
        raise RuntimeError("substrate operators returned empty")
    parsed = json.loads(raw)
    if isinstance(parsed, dict):
        ops = parsed.get("operators", [])
    elif isinstance(parsed, list):
        ops = parsed
    else:
        raise RuntimeError(f"unexpected operators format: {type(parsed)}")
    return [str(op) for op in ops]


def _fetch_operator_capacity(op_addr: str, pw: str) -> dict:
    """Return {active_current, active_maximum} in LUX for a single operator."""
    from .rotation import _cmd
    r = _cmd(f"substrate capacity --operator {op_addr} --format json", timeout=30)
    if not r.get("ok"):
        return {"ok": False, "error": r.get("stderr", "")[:200]}
    raw = (r.get("stdout") or "").strip()
    try:
        obj = json.loads(raw)
    except Exception as e:
        return {"ok": False, "error": f"parse error: {e}"}

    def _get_int(obj, *keys, default=0):
        for k in keys:
            if k in obj:
                return int(obj[k])
        return default

    return {
        "ok":             True,
        "active_current": _get_int(obj, "current_eligibility", "active_current"),
        "active_maximum": _get_int(obj, "max_eligibility",     "active_maximum"),
    }


def is_unstake_target_this_epoch(cur_epoch: int) -> bool:
    """
    Return True if our operator is the current unstake target on substrate.
    Rule: highest max_eligibility among operators with active_current > 0.

    Cached by epoch — first call per epoch queries substrate, subsequent calls
    return cached result until epoch changes.
    """
    global _unstake_target_cache
    with _unstake_cache_lock:
        if _unstake_target_cache.get("epoch") == cur_epoch:
            return bool(_unstake_target_cache.get("is_us"))

    # Cache miss — query substrate
    try:
        from .config import OPERATOR_ADDRESS
        from .rotation import _pw
        our_op = OPERATOR_ADDRESS()
        if not our_op:
            _hlog_warn("unstake-target check: OPERATOR_ADDRESS not configured — assuming NOT us")
            with _unstake_cache_lock:
                _unstake_target_cache = {"epoch": cur_epoch, "is_us": False,
                                          "reason": "no operator addr"}
            return False

        pw = _pw()
        operators = _fetch_operators(pw)
        eligible = []  # (addr, max_elig) where active_current > 0
        for op in operators:
            cap = _fetch_operator_capacity(op, pw)
            if not cap.get("ok"):
                _hlog_warn(f"unstake-target: capacity fetch failed for "
                           f"{op[:12]}…: {cap.get('error','?')[:60]}")
                continue
            if cap["active_current"] > 0:
                eligible.append((op, cap["active_maximum"]))
        if not eligible:
            _hlog_warn("unstake-target check: no operators with active stake — NOT us")
            with _unstake_cache_lock:
                _unstake_target_cache = {"epoch": cur_epoch, "is_us": False,
                                          "reason": "no eligible operators"}
            return False

        # Highest max_eligibility among eligible
        eligible.sort(key=lambda x: -x[1])
        top_addr, top_max = eligible[0]
        is_us = (top_addr == our_op)
        reason = f"top is {top_addr[:12]}… max_elig={top_max:,} LUX"
        with _unstake_cache_lock:
            _unstake_target_cache = {"epoch": cur_epoch, "is_us": is_us,
                                      "reason": reason}
        _hlog(f"unstake-target check (epoch {cur_epoch}): "
              f"{'US' if is_us else 'not us'} — {reason}")
        return is_us

    except Exception as e:
        _hlog_warn(f"unstake-target check failed: {e} — proceeding as NOT us (fail-open)")
        with _unstake_cache_lock:
            _unstake_target_cache = {"epoch": cur_epoch, "is_us": False,
                                      "reason": f"error: {e}"}
        return False


# ── Logging ───────────────────────────────────────────────────────────────────
def _hlog(msg: str, level: str = "info") -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    prefix = {"info": "  ", "warn": "⚠ ", "error": "✖ ",
              "ok": "✓ ", "step": "→ "}.get(level, "  ")
    entry = {"ts": ts, "msg": f"{prefix}{msg}", "level": level}
    with _heal_log_lock:
        _heal_log.appendleft(entry)
    _log(f"[heal] {prefix}{msg}")
    with _heal_state_lock:
        _heal_state["status_msg"] = msg
    try:
        with open(HEAL_LOG_PATH, "a") as f:
            f.write(f"{ts}  {prefix}{msg}\n")
    except Exception:
        pass


def _hlog_ok(m):   _hlog(m, "ok")
def _hlog_warn(m): _hlog(m, "warn")
def _hlog_err(m):  _hlog(m, "error")
def _hlog_step(m): _hlog(m, "step")


# ── Persistence ───────────────────────────────────────────────────────────────
def _save_state() -> None:
    try:
        with _heal_state_lock:
            data = dict(_heal_state)
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        _log(f"[heal] save error: {e}")


def _load_state() -> None:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                saved = json.load(f)
            with _heal_state_lock:
                _heal_state.update(saved)
            _log(f"[heal] loaded persisted state: {saved.get('state')}")
    except Exception as e:
        _log(f"[heal] load error: {e}")


_load_state()


# ── Public API ────────────────────────────────────────────────────────────────
def get_state() -> str:
    with _heal_state_lock:
        return _heal_state["state"]


def get_status() -> dict:
    with _heal_state_lock:
        state = dict(_heal_state)
    with _heal_log_lock:
        log = list(_heal_log)[:100]
    return {**state, "log": log}


def is_in_progress() -> bool:
    return get_state() not in (IDLE, FAILED)


def wants_epoch_n_seed() -> bool:
    """True if rotation window should append a 1k seed of standby."""
    return get_state() == AWAITING_N


def should_run_harvest(cur_epoch: int = None) -> bool:
    """
    True if heal should take this rotation window for harvest (epoch N+1).

    Gates:
      1. State must be SEEDED
      2. Master-heal must be enabled in config
      3. We must NOT be the current unstake target — unless we've deferred
         max_harvest_deferrals times in a row, in which case force-run to
         avoid infinite deferral.
      4. Standby must be ready (ta=1 maturing). Exception: at force-run
         (cap reached), we run anyway — standby may have aged past ta=1 due
         to repeated deferrals, but harvest still needs to happen.

    cur_epoch is needed for the unstake-target gate. If not passed, the gate
    is skipped (for compatibility with callers that don't have epoch handy).
    """
    if get_state() != SEEDED:
        return False

    # Master-heal gate must be enabled
    if not cfg("master_heal_enabled"):
        _hlog_warn("master_heal_enabled=False — not running harvest")
        return False

    # Unstake-target gate (skip if caller didn't pass epoch)
    if cur_epoch is None:
        # Legacy caller — just check standby readiness
        return _standby_ready_for_harvest()

    max_deferrals = int(cfg("max_harvest_deferrals") or 3)
    with _heal_state_lock:
        deferral_count    = int(_heal_state.get("deferral_count") or 0)
        last_defer_epoch  = _heal_state.get("last_deferred_epoch")

    # Reset deferral counter if we've skipped past an epoch without deferring
    if last_defer_epoch is not None and cur_epoch > last_defer_epoch + 1:
        with _heal_state_lock:
            _heal_state["deferral_count"] = 0
            _heal_state["last_deferred_epoch"] = None
        _save_state()
        deferral_count = 0

    # Check readiness + target status
    standby_ok = _standby_ready_for_harvest()
    is_target  = is_unstake_target_this_epoch(cur_epoch)

    if is_target:
        if deferral_count >= max_deferrals:
            _hlog_warn(f"unstake target check: we are target, but already deferred "
                       f"{deferral_count}× — FORCING harvest (cap reached, bypassing "
                       f"readiness check standby_ok={standby_ok})")
            # Reset counter so next cycle starts clean
            with _heal_state_lock:
                _heal_state["deferral_count"] = 0
                _heal_state["last_deferred_epoch"] = None
            _save_state()

            # ── TG: force-run ──
            try:
                from .telegram import alert_heal_force_run
                alert_heal_force_run(deferral_count)
            except Exception as _tg_err:
                _hlog_warn(f"tg alert_heal_force_run failed: {_tg_err}")

            return True   # force-run even if standby has aged past ta=1

        # Defer
        with _heal_state_lock:
            _heal_state["deferral_count"]      = deferral_count + 1
            _heal_state["last_deferred_epoch"] = cur_epoch
            _heal_state["status_msg"] = (
                f"deferred (we are unstake target) — {deferral_count + 1}/{max_deferrals}"
            )
            standby_idx = _heal_state.get("standby_idx", -1)
        _save_state()
        _hlog_warn(f"harvest deferred — we are unstake target this epoch "
                   f"({deferral_count + 1}/{max_deferrals} deferrals; "
                   f"force-run at {max_deferrals})")

        # ── TG: deferred ──
        try:
            from .telegram import alert_heal_deferred
            alert_heal_deferred(standby_idx, deferral_count + 1, max_deferrals)
        except Exception as _tg_err:
            _hlog_warn(f"tg alert_heal_deferred failed: {_tg_err}")

        return False

    # Not the target — need standby readiness to run
    if not standby_ok:
        return False

    # Reset counter and go
    if deferral_count > 0:
        _hlog(f"no longer unstake target (was {deferral_count}× deferred) — "
              f"running harvest")
        with _heal_state_lock:
            _heal_state["deferral_count"]      = 0
            _heal_state["last_deferred_epoch"] = None
        _save_state()
    return True


def reset() -> None:
    """Operator-initiated clear. Admin endpoint."""
    with _heal_state_lock:
        _heal_state.update({
            "state":           IDLE,
            "old_master_idx":  None,
            "standby_idx":     None,
            "triggered_block": None,
            "status_msg":      "reset",
        })
    _save_state()
    _hlog("heal reset — back to IDLE")


# ── Threshold detection (called from rotation._run_state_check) ───────────────
def check_threshold_and_trigger(
        master_idx: int,
        master_stake_dusk: float,
        active_maximum_dusk: float,
        pool_dusk: float,    # kept for API compatibility; heal uses freed stake, not pool
) -> bool:
    """
    Called from rotation's master-threshold branch.
    Returns True if heal is active (rotation should not duplicate alerts).

    Formula:
      rotation_floor     = max(1_000_000, active_max * 0.20)
      target_master      = active_max - rotation_floor
      threshold_dusk     = target_master * (master_threshold_pct / 100)
    """
    if not cfg("master_heal_enabled"):
        return False

    state = get_state()
    if state != IDLE and state != FAILED:
        return True   # already healing

    rotation_floor = max(1_000_000.0, active_maximum_dusk * 0.20)
    target_master  = max(0.0, active_maximum_dusk - rotation_floor)
    # v2 uses master_heal_threshold_pct (new key). Fall back to old master_threshold_pct
    # for backward compatibility if new key is absent.
    threshold_pct  = float(cfg("master_heal_threshold_pct")
                          or cfg("master_threshold_pct")
                          or 50.0)
    threshold_dusk = target_master * (threshold_pct / 100.0)

    if master_stake_dusk >= threshold_dusk:
        return False

    # Find standby — the other slot in {0, 1}
    standby_candidates = [i for i in MASTER_PAIR if i != master_idx]
    if not standby_candidates:
        _hlog_err(f"no standby in master pair {MASTER_PAIR} (master={master_idx})")
        return False
    standby_idx = standby_candidates[0]

    _hlog_step(
        f"★ THRESHOLD TRIPPED: master prov[{master_idx}] stake "
        f"{master_stake_dusk:,.2f} < {threshold_dusk:,.2f} "
        f"(target={target_master:,.0f}, floor={rotation_floor:,.0f})"
    )
    _hlog_step(f"  heal will wake up standby prov[{standby_idx}] at next rotation window")

    with _heal_state_lock:
        _heal_state.update({
            "state":           AWAITING_N,
            "old_master_idx":  master_idx,
            "standby_idx":     standby_idx,
            "triggered_block": _current_block(),
        })
    _save_state()

    # ── TG: heal triggered ──
    try:
        from .telegram import alert_heal_triggered
        alert_heal_triggered(master_idx, standby_idx, master_stake_dusk,
                             threshold_dusk, threshold_pct)
    except Exception as _tg_err:
        _hlog_warn(f"tg alert_heal_triggered failed: {_tg_err}")

    return True


# ── Epoch-N rotation window: seed standby (called from rotation._run_rotation) ──
def perform_epoch_n_seed(cur_epoch: int) -> bool:
    """
    Append an extra `stake_activate(standby, 1k)` to rotation's window sequence.
    Called AFTER rotation's step 4 (re-seed rot_active) has confirmed.

    This function waits BLOCK_GAP blocks after the most recent confirmed
    block before firing, to preserve the inter-tx gap guarantee.

    Returns True on success (state advances to SEEDED), False on failure.
    """
    if get_state() != AWAITING_N:
        _hlog_warn(f"perform_epoch_n_seed called but state={get_state()} — skipping")
        return False

    with _heal_state_lock:
        standby_idx = _heal_state["standby_idx"]

    try:
        from .rotation import _addr
        from .assess   import _assess_state_cached
        nodes = _assess_state_cached(0, "").get("by_idx", {})
        addr = _addr(standby_idx, nodes)
        if not addr:
            _hlog_err(f"epoch-N seed: no address for prov[{standby_idx}]")
            return False

        # Wait the block gap since last tx from rotation's step 4.
        # We don't have rotation's executed_block handy, so wait for "any"
        # next block_accepted as a safe proxy (block gap 1).
        from .nodes import get_heights
        heights = get_heights()
        last_seen = max(heights.values(), default=0) if heights else 0
        if last_seen > 0:
            from .rues import wait_for_block
            target = last_seen + BLOCK_GAP
            _hlog_step(f"epoch N: waiting for block {target} before standby seed")
            wait_for_block(target, timeout=60)

        _hlog_step(f"epoch N: seeding standby prov[{standby_idx}] with {SEED_DUSK:.0f} DUSK")
        ok = _fire_and_gap("stake_activate", addr, amount_dusk=SEED_DUSK)
        if not ok:
            _mark_failed()
            return False

        _hlog_ok(f"epoch N: standby prov[{standby_idx}] seeded — advancing to SEEDED")
        with _heal_state_lock:
            _heal_state["state"] = SEEDED
            old_master_idx = _heal_state.get("old_master_idx", -1)
        _save_state()

        # ── TG: seeded ──
        try:
            from .telegram import alert_heal_seeded
            alert_heal_seeded(old_master_idx, standby_idx)
        except Exception as _tg_err:
            _hlog_warn(f"tg alert_heal_seeded failed: {_tg_err}")

        return True

    except Exception as e:
        _hlog_err(f"epoch-N seed error: {e}")
        _mark_failed()
        return False


# ── Epoch-N+1 rotation window: harvest (called from rotation._run_rotation) ──
def run_harvest(cur_epoch: int) -> None:
    """
    Full harvest sequence in epoch N+1 rotation window. 5 steps, each waiting
    for tx/executed, then waiting for block_accepted >= executed + BLOCK_GAP
    before firing the next.

    Runs on rotation's _run_rotation thread; blocks that thread for the
    duration of the harvest (~1-3 min within a 6.8-min window).
    """
    if get_state() != SEEDED:
        _hlog_warn(f"run_harvest called but state={get_state()} — aborting")
        return

    with _heal_state_lock:
        old_master_idx = _heal_state["old_master_idx"]
        standby_idx    = _heal_state["standby_idx"]
        _heal_state["state"] = HARVESTING
    _save_state()
    _hlog_step(f"═══ HEAL HARVEST epoch {cur_epoch} ═══")

    try:
        from .rotation import _addr, _cmd, _pw, _rot_indices
        from .assess   import _assess_state_cached, _fetch_capacity_cached
        from .wallet   import WALLET_PATH
        from .rues     import register_tx_confirm, get_tx_confirm_result, wait_for_block

        nodes = _assess_state_cached(0, "").get("by_idx", {})
        master_addr  = _addr(old_master_idx, nodes)
        standby_addr = _addr(standby_idx, nodes)

        # Find current rot_active (ta=0) AND rot_slave (ta=1, becoming next rot_active).
        # Important: bulk allocation goes to rot_slave (next rot_active), not to the
        # just-liquidated rot_active. The just-liquidated node only needs a 1k re-seed.
        rot = _rot_indices()
        rot_active_idx = next(
            (i for i in rot if nodes.get(i, {}).get("status") == "active"
             and nodes.get(i, {}).get("ta") == 0),
            None
        )
        rot_slave_idx = next(
            (i for i in rot if nodes.get(i, {}).get("status") in ("maturing", "seeded")
             and nodes.get(i, {}).get("ta") == 1),
            None
        )
        if rot_active_idx is None:
            _hlog_err("run_harvest: no rot_active found — cannot proceed")
            _mark_failed()
            return
        if rot_slave_idx is None:
            _hlog_err("run_harvest: no rot_slave (ta=1) found — cannot proceed; "
                      "no node available to receive bulk allocation as next rot_active")
            _mark_failed()
            return
        rot_active_addr = _addr(rot_active_idx, nodes)
        rot_slave_addr  = _addr(rot_slave_idx, nodes)

        # Capacity for target allocation
        cap = _fetch_capacity_cached(_pw())
        active_max     = cap.get("active_maximum", 0.0)
        # rotation_floor_pct is the single knob that controls the split.
        # target_master = active_max × (1 - rot_floor_pct/100).
        rot_floor_pct  = float(cfg("rotation_floor_pct") or 20.0)
        rotation_floor = active_max * (rot_floor_pct / 100.0)
        target_master  = max(0.0, active_max - rotation_floor)

        # Accumulate freed stake as we go
        freed_total = 0.0

        # ── Step 1: liquidate old master ─────────────────────────────────────
        # Locked DOES return to pool on liquidate (it doesn't earn rewards
        # while locked, but it's not slashed/burned — it's released).
        stake_master = nodes.get(old_master_idx, {}).get("stake_dusk", 0.0)
        locked_master = nodes.get(old_master_idx, {}).get("locked_dusk", 0.0)
        _hlog_step(f"[1/7] liquidate master prov[{old_master_idx}] "
                   f"(stake={stake_master:.0f}, locked={locked_master:.0f})")
        if not _fire_and_gap("liquidate", master_addr):
            _mark_failed(); return
        freed_master = stake_master + locked_master
        freed_total += freed_master
        _hlog_ok(f"[1/7] master drained — {freed_master:.0f} DUSK freed "
                 f"(stake={stake_master:.0f} + locked={locked_master:.0f})")

        # ── Step 2: terminate old master ─────────────────────────────────────
        rewards_master = nodes.get(old_master_idx, {}).get("reward_dusk", 0.0)
        _hlog_step(f"[2/7] terminate master prov[{old_master_idx}] "
                   f"(rewards={rewards_master:.4f})")
        if not _fire_and_gap("terminate", master_addr):
            _mark_failed(); return
        freed_total += rewards_master
        _hlog_ok(f"[2/7] master rewards freed — {rewards_master:.4f} DUSK")

        # ── Step 3: liquidate rot_active ─────────────────────────────────────
        stake_rot      = nodes.get(rot_active_idx, {}).get("stake_dusk", 0.0)
        locked_rot     = nodes.get(rot_active_idx, {}).get("locked_dusk", 0.0)
        rewards_rot    = nodes.get(rot_active_idx, {}).get("reward_dusk", 0.0)
        _hlog_step(f"[3/7] liquidate rot_active prov[{rot_active_idx}] "
                   f"(stake={stake_rot:.0f}, locked={locked_rot:.0f})")
        if not _fire_and_gap("liquidate", rot_active_addr):
            _mark_failed(); return
        freed_rot = stake_rot + locked_rot
        freed_total += freed_rot
        _hlog_ok(f"[3/7] rot_active drained — {freed_rot:.0f} DUSK freed "
                 f"(stake={stake_rot:.0f} + locked={locked_rot:.0f}); "
                 f"cumulative freed={freed_total:.0f}")

        # ── Step 4: terminate rot_active (free rewards + clear provisioner record) ──
        # Without this, step 6 (re-seed rot_active) panics with "provisioner
        # should be eligible for activation" — the post-liquidate provisioner
        # state is not eligible until terminate completes the cleanup.
        _hlog_step(f"[4/7] terminate rot_active prov[{rot_active_idx}] "
                   f"(rewards={rewards_rot:.4f})")
        if not _fire_and_gap("terminate", rot_active_addr):
            _mark_failed(); return
        freed_total += rewards_rot
        _hlog_ok(f"[4/7] rot_active rewards freed — {rewards_rot:.4f} DUSK")

        # ── Allocation priority ──────────────────────────────────────────────
        # Rotation nodes have one epoch as rot_active to earn — so the rot_slave
        # (becoming next rot_active at epoch boundary) must hit rot_floor.
        # Master stays active across many epochs and can recover via deposits;
        # it gets the remainder up to target_master.
        #
        # Existing stake at this point:
        #   - standby has SEED_DUSK (1k) from epoch N seed
        #   - rot_slave (ta=1) has whatever was on it (typically 1k seed from
        #     the prior epoch's rotation)
        #   - rot_active just got liquidated to 0; needs only a 1k re-seed
        # Remaining budget: freed_total to split between rot_slave (up to floor)
        # and standby (up to target_master).
        #
        # Step 4: fill rot_slave to rot_floor          (becomes next rot_active)
        # Step 5: re-seed rot_active with 1k           (just-liquidated; new rot_seeded)
        # Step 6: fill standby with remainder          (becomes new master)

        # ── Step 5: top up rot_slave to rot_floor ────────────────────────────
        # rot_slave is becoming next rot_active. It needs to hit rot_floor for
        # the next epoch's rotation cycle to work properly. The rot_slave
        # already has some existing stake (typically 1k seed from prior epoch);
        # we top it up by (rot_floor - existing_stake).
        existing_rotslave = nodes.get(rot_slave_idx, {}).get("stake_dusk", 0.0)
        rotslave_topup    = max(0.0, rotation_floor - existing_rotslave)
        rotslave_alloc    = min(freed_total, rotslave_topup)
        _hlog_step(f"[5/7] top up rot_slave prov[{rot_slave_idx}] with "
                   f"{rotslave_alloc:,.2f} DUSK (existing={existing_rotslave:,.0f}, "
                   f"target={rotation_floor:,.0f}; becomes next rot_active)")
        if rotslave_alloc < 10.0:
            _hlog_warn(f"[5/7] insufficient to top up rot_slave "
                       f"({rotslave_alloc:.2f}) — skipping")
        else:
            if not _fire_and_gap("stake_activate", rot_slave_addr,
                                 amount_dusk=rotslave_alloc):
                _mark_failed(); return
            _hlog_ok(f"[5/7] rot_slave topped up {rotslave_alloc:,.2f} DUSK")
            freed_total -= rotslave_alloc

        # ── Step 6: re-seed just-liquidated-and-terminated rot_active with 1k ──
        # rot_active was liquidated to 0. It just needs the standard 1k seed
        # to start the maturing cycle (ta=2 → ta=1 → ta=0 over 3 epochs).
        rotactive_seed = SEED_DUSK
        if freed_total < rotactive_seed:
            _hlog_warn(f"[6/7] insufficient for rot_active reseed "
                       f"({freed_total:.2f} < {rotactive_seed:.0f}) — skipping; "
                       f"state check will retry seeding from pool")
        else:
            _hlog_step(f"[6/7] re-seed rot_active prov[{rot_active_idx}] with "
                       f"{rotactive_seed:,.0f} DUSK (was just liquidated+terminated)")
            if not _fire_and_gap("stake_activate", rot_active_addr,
                                 amount_dusk=rotactive_seed):
                _mark_failed(); return
            _hlog_ok(f"[6/7] rot_active re-seeded {rotactive_seed:.0f} DUSK")
            freed_total -= rotactive_seed

        # ── Step 7: top up standby to target_master with remainder ──────────
        # Standby already has SEED_DUSK from epoch N. Space remaining is
        # target_master - SEED_DUSK.
        space_on_master = max(0.0, target_master - SEED_DUSK)
        master_alloc    = min(freed_total, space_on_master)
        _hlog_step(f"[7/7] top up standby prov[{standby_idx}] with "
                   f"{master_alloc:,.2f} DUSK (target={target_master:,.0f}; "
                   f"becomes new master)")
        if master_alloc < 10.0:
            _hlog_warn(f"[7/7] insufficient to top up master ({master_alloc:.2f}) — "
                       f"skipping; master will run undersized until sweeper refills")
        else:
            if not _fire_and_gap("stake_activate", standby_addr,
                                 amount_dusk=master_alloc):
                _mark_failed(); return
            _hlog_ok(f"[7/7] standby topped up with {master_alloc:,.2f} DUSK")
            freed_total -= master_alloc

        if freed_total > 1.0:
            _hlog_warn(f"  ↳ {freed_total:,.2f} DUSK excess remained after harvest — "
                       f"will be picked up by snatch window sweeper")

        # ── Done — advance to COMPLETING ────────────────────────────────────
        _hlog_ok(f"═══ HEAL HARVEST epoch {cur_epoch} complete — awaiting epoch boundary ═══")
        with _heal_state_lock:
            _heal_state["state"] = COMPLETING
            old_master_idx = _heal_state.get("old_master_idx", -1)
            new_master_idx = _heal_state.get("standby_idx", -1)
        _save_state()

        # ── TG: harvest complete (role swap pending) ──
        try:
            from .telegram import alert_heal_harvest_complete
            # Estimate new master stake: existing seed + master_alloc if we ran step 5
            new_master_stake = SEED_DUSK + (master_alloc if master_alloc >= 10.0 else 0)
            alert_heal_harvest_complete(old_master_idx, new_master_idx, new_master_stake)
        except Exception as _tg_err:
            _hlog_warn(f"tg alert_heal_harvest_complete failed: {_tg_err}")

    except Exception as e:
        _hlog_err(f"harvest error: {e}")
        _mark_failed()


# ── Block-level tick (called from rotation.on_block) ──────────────────────────
def tick_heal(block_height: int) -> None:
    """
    Per-block entry point. Handles COMPLETING → IDLE role swap at epoch boundary.
    All other state transitions are driven by rotation window hooks (not per-block).
    """
    if not cfg("master_heal_enabled"):
        return
    state = get_state()

    # Role swap at epoch boundary
    if state == COMPLETING and block_height % EPOCH_BLOCKS == 0:
        _perform_role_swap(block_height)


# ── Internal helpers ──────────────────────────────────────────────────────────
def _fire_and_gap(fn_name: str, addr: str, amount_dusk: float = 0.0) -> bool:
    """
    Fire a pool op, wait for tx/executed, then wait for block_accepted to be
    >= executed_block + BLOCK_GAP. Returns True on success, False on any failure
    (CLI error, tx reverted, timeout).

    fn_name: "liquidate" | "terminate" | "stake_activate"
    amount_dusk: required for stake_activate, ignored otherwise.
    """
    from .rotation import _cmd, _pw
    from .wallet   import WALLET_PATH
    from .rues     import register_tx_confirm, get_tx_confirm_result, wait_for_block

    evt = register_tx_confirm(fn_name, addr)

    if fn_name == "liquidate":
        cmd = f"pool liquidate --skip-confirmation --provisioner {addr}"
    elif fn_name == "terminate":
        cmd = f"pool terminate --skip-confirmation --provisioner {addr}"
    elif fn_name == "stake_activate":
        lux = round(amount_dusk * 1e9)
        cmd = (f"pool stake-activate --skip-confirmation "
               f"--amount {lux} --provisioner {addr} "
               f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{_pw()}'")
    else:
        _hlog_err(f"_fire_and_gap: unknown fn_name {fn_name!r}")
        return False

    r = _cmd(cmd)
    if not r.get("ok"):
        err = r.get("stderr", "")[:120]
        if fn_name == "liquidate" and any(x in err.lower() for x in
                                          ("no stake", "does not exist", "nothing to liquidate")):
            _hlog_warn(f"{fn_name} {addr[:16]}…: already empty — proceeding")
            return True   # nothing to do; advance
        _hlog_err(f"{fn_name} CLI failed: {err}")
        return False

    if not evt.wait(timeout=120):
        _hlog_err(f"{fn_name} tx/executed timeout")
        return False
    res = get_tx_confirm_result(fn_name, addr)
    if res and res.get("err"):
        _hlog_err(f"{fn_name} reverted on-chain: {str(res['err'])[:120]}")
        return False
    executed_block = (res or {}).get("block_height", 0)
    if not executed_block:
        _hlog_warn(f"{fn_name} confirmed but no block_height in result — skipping gap wait")
        return True

    # Wait for block_accepted >= executed + BLOCK_GAP
    target = executed_block + BLOCK_GAP
    _hlog_step(f"  ↳ {fn_name} executed at block {executed_block}; waiting for block {target}")
    if not wait_for_block(target, timeout=60):
        _hlog_err(f"{fn_name} block-gap wait timeout (target {target})")
        return False
    return True


def _standby_ready_for_harvest() -> bool:
    """True if standby_idx is at ta=1 (can become active next epoch)."""
    try:
        from .assess import _assess_state_cached
        with _heal_state_lock:
            standby_idx = _heal_state["standby_idx"]
        if standby_idx is None:
            return False
        nodes = _assess_state_cached(0, "").get("by_idx", {})
        n = nodes.get(standby_idx, {})
        return (n.get("status") in ("maturing", "seeded") and n.get("ta") == 1)
    except Exception:
        return False


def _perform_role_swap(block_height: int) -> None:
    """Called when an epoch boundary is crossed while state == COMPLETING.
    Verifies standby is now ACTIVE, updates cfg master_idx, resets state."""
    try:
        from .assess import _assess_state_cached
        nodes = _assess_state_cached(0, "").get("by_idx", {})
    except Exception as e:
        _hlog_warn(f"role swap: assess error {e}")
        return

    with _heal_state_lock:
        standby_idx    = _heal_state["standby_idx"]
        old_master_idx = _heal_state["old_master_idx"]

    if standby_idx is None or old_master_idx is None:
        _hlog_err("role swap: missing indices — resetting")
        reset()
        return

    n = nodes.get(standby_idx, {})
    if n.get("status") != "active":
        # Not active yet at this boundary — wait one more epoch
        return

    _hlog_ok(f"★ HEAL COMPLETE: prov[{standby_idx}] is now MASTER "
             f"(was prov[{old_master_idx}])")

    # Update cfg
    try:
        from .config import _cfg, _save_config
        _cfg["master_idx"] = standby_idx
        try:
            _save_config(_cfg)
        except Exception:
            pass
        _hlog_ok(f"config master_idx updated: {old_master_idx} → {standby_idx}")
    except Exception as e:
        _hlog_err(f"config master_idx update failed: {e}")

    with _heal_state_lock:
        _heal_state.update({
            "state":           IDLE,
            "old_master_idx":  None,
            "standby_idx":     None,
            "triggered_block": None,
            "deferral_count":  0,
            "last_deferred_epoch": None,
        })
    _save_state()

    # ── TG: heal complete ──
    try:
        from .telegram import alert_heal_complete
        new_stake = nodes.get(standby_idx, {}).get("stake_dusk", 0.0)
        alert_heal_complete(standby_idx, new_stake)
        # Clear alert-threshold suppression so future drops re-alert cleanly
        from .telegram import reset_alert
        reset_alert("master_below_alert_threshold")
        reset_alert("heal_triggered")
        reset_alert("heal_seeded")
        reset_alert("heal_harvest_complete")
        reset_alert("heal_failed")
    except Exception as _tg_err:
        _hlog_warn(f"tg alert_heal_complete failed: {_tg_err}")


def _mark_failed() -> None:
    """Move to FAILED state — next threshold check will reset to IDLE."""
    with _heal_state_lock:
        _heal_state["state"] = FAILED
        last_step = _heal_state.get("status_msg", "unknown step")
    _save_state()
    try:
        from .telegram import alert_heal_failed
        alert_heal_failed(last_step)
    except Exception:
        pass


def _current_block() -> int:
    try:
        from .nodes import get_heights
        heights = get_heights()
        return max(heights.values(), default=0) if heights else 0
    except Exception:
        return 0
