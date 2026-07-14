"""
redistribute.py — Manual "consolidate-and-hop" stake redistribution.

PURPOSE
-------
Over time the rotation node (rot_master) accumulates far more stake than it
needs (e.g. 3.6M when only 1M is wanted for rotation). Under normal rotation
that whole amount is liquidated + re-staked every epoch, and it only ever
earns on the "rotation" role. The excess would be better parked on a large
"master-tier" node that never gets recycled.

This module moves the excess out of the rotation pair and onto the *other*
master-pair node, WITHOUT the 10% top-up slash, by exploiting the same
mechanic rotation already relies on: topping up a ta==1 (maturing) node keeps
it maturing and it activates at the next epoch boundary — no slash, and the
big amount is only "in flight" for the rotation window, not two full epochs.

CONSOLIDATE-AND-HOP
-------------------
The master role hops between the two MASTER_PAIR nodes. Whichever is NOT the
current active master is the landing zone for the next redistribution. After
each redistribution the old master node is freed (inactive), ready to receive
the next one. This recycles forever with only the existing 4 nodes.

  cur_master = the MASTER_PAIR node that is active now (untouched until the
               very last step)
  new_master = the other MASTER_PAIR node (inactive → pre-seeded → topped up)

TWO-EPOCH FLOW
--------------
Epoch N   (rotation window): normal rotation runs, PLUS pre-seed new_master
          with 1000 DUSK so it enters the maturation pipeline (ta=2 this epoch).
Boundary  N→N+1: new_master ta 2 → 1.
Epoch N+1 (rotation window, before snatch): the consolidate OWNS the whole
          window (normal rotation is skipped) and runs the full 7-tx sequence,
          mirroring heal's harvest ordering:
    1. confirm new_master is ta==1 (no-master guard)
    2. liquidate + terminate cur_master → pool
    3. liquidate + terminate rot_master (rot_active) → pool   (frees the excess)
    4. top-up new_master (ta==1) with excess + cur_master stake  ← no slash
       on failure: fallback ladder (rot_slave ta==1 → any ta==1 → any node)
    5. top-up rot_slave (ta==1) → target                     (next rot_master)
    6. re-seed rot_active with 1k → ta=2
Both liquidations happen BEFORE the new_master top-up so the pool actually holds
the excess — otherwise the pool-balance clamp silently drops it. rot_master must
be freed here (not delegated to normal rotation, which ignores `target` and
would recycle the whole excess back into the rotation pair).
Boundary  N+1→N+2: new_master active (excess+master), cur_master freed,
          rot_slave active as rot_master at target. Master has hopped.

GUARDS
------
- Fronting-target defer: if the node we'd pre-seed sits at/ahead of the current
  master in the unstake fronting order AND we are the unstake target this epoch,
  the 1k pre-seed would be eaten. Defer to a later epoch. Reuses
  heal.is_unstake_target_this_epoch().
- No-master: never liquidate cur_master until new_master is confirmed ta==1.
- Snatch window: all epoch-N+1 txs must complete before the snatch window
  (blk_left <= snatch_win) so liquidated stake is never exposed to the grab.
- Capacity: query operator capacity fresh before every stake_activate and clamp
  to active headroom. Never top up an ACTIVE node (would slash).

FAILURE RECOVERY (highest priority)
-----------------------------------
If cur_master is liquidated but the new_master top-up fails, the master stake
must not be stranded in the pool through the snatch window. Recovery ladder:
    1. retry new_master (ta==1) once or twice   — cleanest, active in 1 epoch
    2. rot_slave (ta==1)                          — active in 1 epoch
    3. any other ta==1 node                       — active in 1 epoch
    4. any node (even inactive → 2 epochs)        — safe, slower
Idle for 2 epochs beats losing master stake in the snatch race.

This module owns state file ~/.sozu_redistribute.json and mirrors the heal.py
persistence + logging conventions. It is driven from rotation._run_rotation
via two hooks (preseed at epoch N, consolidate at epoch N+1).
"""

import json
import os
import threading
import time

from .config import cfg, _log, MASTER_PAIR, ROTATION_PAIR, NODE_INDICES


# ── Constants ─────────────────────────────────────────────────────────────────
SEED_DUSK           = 1000.0
SEED_LUX            = round(SEED_DUSK * 1e9)
MIN_STAKE_DUSK      = 1000.0          # below this a node is treated as inactive
DEFAULT_TARGET_DUSK = 1_000_000.0     # default rot_master target
TX_CONFIRM_TIMEOUT  = 120             # seconds to wait for tx/executed
TOPUP_RETRIES       = 2               # retries on the primary new_master top-up

STATE_FILE = os.path.expanduser("~/.sozu_redistribute.json")
LOG_PATH   = os.path.expanduser("~/.sozu_redistribute.log")

# ── States ────────────────────────────────────────────────────────────────────
IDLE       = "idle"        # nothing armed
ARMED      = "armed"       # operator armed; waiting for a clean epoch to pre-seed
PRESEEDED  = "preseeded"   # 1k pre-seed placed on new_master; waiting for ta==1
DONE       = "done"        # transient — reset to IDLE after consolidate

_state_default = {
    "state":          IDLE,
    "target_dusk":    DEFAULT_TARGET_DUSK,
    "new_master_idx": None,   # node being grown into the next master
    "cur_master_idx": None,   # the active master at arm time (moves last)
    "armed_epoch":    None,
    "preseed_epoch":  None,
    "status_msg":     "",
}

_rd_state      = dict(_state_default)
_rd_state_lock = threading.Lock()

_rd_log        = []
_rd_log_lock   = threading.Lock()


# ── Logging (mirrors heal.py) ─────────────────────────────────────────────────
def _rdlog(msg: str, level: str = "info") -> None:
    line = {"ts": time.time(), "level": level, "msg": msg}
    with _rd_log_lock:
        _rd_log.insert(0, line)
        del _rd_log[500:]
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{level}] {msg}\n")
    except Exception:
        pass
    _log(f"[redistribute] {msg}")


def _rdlog_ok(m):   _rdlog(m, "ok")
def _rdlog_warn(m): _rdlog(m, "warn")
def _rdlog_err(m):  _rdlog(m, "error")
def _rdlog_step(m): _rdlog(m, "step")
def _rdlog_info(m): _rdlog(m, "info")


# ── Persistence ───────────────────────────────────────────────────────────────
def _save_state() -> None:
    try:
        with _rd_state_lock:
            data = dict(_rd_state)
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        _log(f"[redistribute] save error: {e}")


def _load_state() -> None:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                saved = json.load(f)
            with _rd_state_lock:
                _rd_state.update(saved)
            _log(f"[redistribute] loaded persisted state: {saved.get('state')}")
    except Exception as e:
        _log(f"[redistribute] load error: {e}")


_load_state()


# ── Public API ────────────────────────────────────────────────────────────────
def get_state() -> str:
    with _rd_state_lock:
        return _rd_state["state"]


def get_status() -> dict:
    with _rd_state_lock:
        state = dict(_rd_state)
    with _rd_log_lock:
        log = list(_rd_log)[:100]
    return {**state, "log": log}


def reset() -> None:
    """Operator-initiated clear back to IDLE. Does NOT touch on-chain stake."""
    with _rd_state_lock:
        _rd_state.update(dict(_state_default))
        _rd_state["status_msg"] = "reset"
    _save_state()
    _rdlog("redistribute reset — back to IDLE")


def arm(target_dusk: float | None = None) -> dict:
    """Arm a redistribution. Executes across the next two suitable rotation
    windows. `target_dusk` is the desired rot_master stake after redistribution
    (default 1,000,000). Returns the resulting status dict.
    """
    if get_state() != IDLE:
        return {"ok": False, "error": f"redistribution already active (state={get_state()})"}

    target = float(target_dusk) if target_dusk else DEFAULT_TARGET_DUSK
    if target < SEED_DUSK:
        return {"ok": False, "error": f"target must be >= {SEED_DUSK:.0f} DUSK"}

    with _rd_state_lock:
        _rd_state.update({
            "state":          ARMED,
            "target_dusk":    target,
            "new_master_idx": None,
            "cur_master_idx": None,
            "armed_epoch":    None,
            "preseed_epoch":  None,
            "status_msg":     f"armed — target rot_master {target:,.0f} DUSK, "
                              f"waiting for next rotation window",
        })
    _save_state()
    _rdlog_step(f"armed — target rot_master {target:,.0f} DUSK")
    return {"ok": True, **get_status()}


# ── Helpers that reuse rotation.py primitives ─────────────────────────────────
def _rot_helpers():
    """Lazy import of rotation internals (avoids import cycle at module load)."""
    from . import rotation as R
    return R


def _fronting_order_position(idx: int) -> int:
    """Lower = hit earlier by unstakes. The chain fronts unstakes from an
    operator's provisioners in ascending node-index order, so position == idx.
    (Kept as a function so the rule lives in one place.)
    """
    return idx


def _new_master_exposed_to_fronting(new_idx: int, cur_master_idx: int) -> bool:
    """True if new_master sits at/ahead of cur_master in the fronting order,
    i.e. its 1k pre-seed could be eaten before a larger node absorbs unstakes.
    """
    return _fronting_order_position(new_idx) <= _fronting_order_position(cur_master_idx)


def _capacity(pw: str) -> dict:
    from .assess import _fetch_capacity
    return _fetch_capacity(pw)


def _active_headroom_dusk(cap: dict) -> float:
    return max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))


def _preview_amounts(nodes: dict, target_dusk: float,
                     rot_active_idx: int, cur_master_idx: int) -> dict:
    """Compute the amounts a redistribution would move, for the preview UI and
    for the executor. All DUSK.
    """
    rot_stake  = nodes.get(rot_active_idx, {}).get("stake_dusk", 0.0)
    rot_locked = nodes.get(rot_active_idx, {}).get("locked_dusk", 0.0)
    mas_stake  = nodes.get(cur_master_idx, {}).get("stake_dusk", 0.0)
    mas_locked = nodes.get(cur_master_idx, {}).get("locked_dusk", 0.0)
    # The consolidate liquidates BOTH cur_master and rot_master, freeing each
    # node's stake AND locked collateral to the pool; everything above `target`
    # lands on the new master. Count locked on BOTH nodes: omitting the ROT
    # node's locked stranded it in the pool → snatch-exposed (a ~10k loss on the
    # first mainnet run); omitting the master's understated the size (~950k on
    # testnet). excess therefore includes rot_locked.
    excess     = max(0.0, rot_stake + rot_locked - target_dusk)
    new_master = excess + mas_stake + mas_locked
    return {
        "rot_stake":       rot_stake,
        "rot_locked":      rot_locked,
        "master_stake":    mas_stake,
        "master_locked":   mas_locked,
        "target":          target_dusk,
        "excess":          excess,
        "new_master_size": new_master,
    }


# ── Phase hooks — called from rotation._run_rotation ──────────────────────────
def wants_preseed(cur_epoch: int) -> bool:
    """True if a redistribution is ARMED and this epoch's rotation window should
    also pre-seed the new_master node. Applies the fronting-target defer guard.
    """
    if get_state() != ARMED:
        return False

    R = _rot_helpers()
    try:
        st    = R._assess()
        nodes = st.get("by_idx", {})
        cur_master_idx = R._master_idx()
        if cur_master_idx not in MASTER_PAIR:
            _rdlog_warn(f"preseed check: master_idx {cur_master_idx} not in MASTER_PAIR — deferring")
            return False

        # new_master = the OTHER master-pair node
        new_master_idx = next((i for i in MASTER_PAIR if i != cur_master_idx), None)
        if new_master_idx is None:
            _rdlog_warn("preseed check: could not resolve new_master — deferring")
            return False

        # Single-owner guard: heal and redistribute both target the master-pair
        # standby (prov[new_master_idx]). If a heal cycle is mid-flight on the
        # same node, its seed/harvest would corrupt our maturation clock and
        # liquidate the master out from under us (the epoch-1743 skip). Defer.
        try:
            from .heal import is_in_progress, get_status as _heal_status
            if is_in_progress() and _heal_status().get("standby_idx") == new_master_idx:
                _rdlog_warn(f"preseed deferred: heal is mid-cycle on prov[{new_master_idx}] "
                            f"(standby) — waiting for heal to clear before touching it")
                return False
        except Exception as _he:
            _rdlog_warn(f"preseed: heal-ownership check failed ({_he}) — proceeding cautiously")

        # It must currently be inactive (a clean landing zone).
        new_status = nodes.get(new_master_idx, {}).get("status")
        if new_status != "inactive":
            _rdlog_warn(f"preseed: new_master prov[{new_master_idx}] is '{new_status}', "
                        f"expected inactive — deferring (free it first)")
            return False

        # Fronting-target defer: only an issue if new_master is exposed AND we
        # are the unstake target this epoch.
        if _new_master_exposed_to_fronting(new_master_idx, cur_master_idx):
            try:
                from .heal import is_unstake_target_this_epoch
                if is_unstake_target_this_epoch(cur_epoch):
                    _rdlog_warn(f"preseed deferred: prov[{new_master_idx}] is the fronting node "
                                f"and we are the unstake target this epoch — the 1k seed would be "
                                f"eaten. Waiting for a clear epoch.")
                    return False
            except Exception as _fe:
                _rdlog_warn(f"preseed: fronting-target check failed ({_fe}) — proceeding cautiously")

        # Record the resolved roles for the consolidate phase.
        with _rd_state_lock:
            _rd_state["cur_master_idx"] = cur_master_idx
            _rd_state["new_master_idx"] = new_master_idx
        return True
    except Exception as e:
        _rdlog_warn(f"preseed check error: {e} — deferring")
        return False


def perform_preseed(cur_epoch: int) -> None:
    """Seed the new_master node with 1000 DUSK so it starts maturing. Called
    during epoch N's rotation window after the normal rotation steps.
    """
    R = _rot_helpers()
    from .wallet import WALLET_PATH
    from .rues import register_tx_confirm, get_tx_confirm_result

    with _rd_state_lock:
        new_master_idx = _rd_state.get("new_master_idx")
        cur_master_idx = _rd_state.get("cur_master_idx")

    if new_master_idx is None:
        _rdlog_err("perform_preseed: new_master_idx not set — aborting")
        return

    st    = R._assess()
    nodes = st.get("by_idx", {})
    addr  = R._addr(new_master_idx, nodes)
    if not addr:
        _rdlog_err(f"perform_preseed: no address for prov[{new_master_idx}] — aborting")
        return

    # Capacity guard — seeding adds SEED_DUSK to active_current (maturing counts).
    cap = _capacity(R._pw())
    if _active_headroom_dusk(cap) < SEED_DUSK:
        _rdlog_err(f"perform_preseed: only {_active_headroom_dusk(cap):.0f} DUSK active headroom "
                   f"< {SEED_DUSK:.0f} seed — cannot pre-seed, staying ARMED")
        return

    _rdlog_step(f"pre-seed prov[{new_master_idx}] with {SEED_DUSK:.0f} DUSK → ta=2 "
                f"(new master, cur master prov[{cur_master_idx}])")
    seed_evt = register_tx_confirm("stake_activate", addr)
    r = R._cmd(
        f"pool stake-activate --skip-confirmation "
        f"--amount {SEED_LUX} --provisioner {addr} "
        f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{R._pw()}'")
    try:
        from .assess import _invalidate_capacity_cache
        _invalidate_capacity_cache()
    except Exception:
        pass

    if not r.get("ok"):
        _rdlog_err(f"pre-seed FAILED (CLI): {r.get('stderr','')[:200]} — staying ARMED, will retry")
        return
    if not seed_evt.wait(timeout=TX_CONFIRM_TIMEOUT):
        _rdlog_warn("pre-seed confirmation timeout — will verify state next epoch")
    else:
        res = get_tx_confirm_result("stake_activate", addr)
        if (res or {}).get("err"):
            _rdlog_err(f"pre-seed REVERTED on-chain: {str(res['err'])[:200]} — staying ARMED")
            return
        _rdlog_ok(f"pre-seed confirmed — prov[{new_master_idx}] now ta=2")

    with _rd_state_lock:
        _rd_state["state"]         = PRESEEDED
        _rd_state["preseed_epoch"] = cur_epoch
        _rd_state["armed_epoch"]   = _rd_state.get("armed_epoch") or cur_epoch
        _rd_state["status_msg"]    = (f"pre-seeded prov[{new_master_idx}]; consolidate next "
                                      f"rotation window once it reaches ta==1")
    _save_state()


def wants_consolidate(cur_epoch: int) -> bool:
    """True if we are PRESEEDED and the new_master has reached ta==1 — meaning
    the consolidate can run in THIS rotation window. If new_master is not yet
    ta==1 (should be ta==1 exactly one epoch after pre-seed), we do NOT touch
    the master; we hold and re-check next epoch.
    """
    if get_state() != PRESEEDED:
        return False

    R = _rot_helpers()
    try:
        st    = R._assess()
        nodes = st.get("by_idx", {})
        with _rd_state_lock:
            new_master_idx = _rd_state.get("new_master_idx")
        ta = nodes.get(new_master_idx, {}).get("ta")
        if ta == 1:
            return True
        # ta==2 still maturing (pre-seed happened this same epoch, or a boundary
        # was missed) → wait. ta==0/None/anything else → the seed was lost.
        if ta == 2:
            _rdlog_warn(f"consolidate hold: prov[{new_master_idx}] still ta=2 — "
                        f"waiting one more epoch for ta==1")
        else:
            _rdlog_err(f"consolidate ABORT: prov[{new_master_idx}] ta={ta} "
                       f"(expected 1) — pre-seed was lost. Master untouched. Resetting.")
            _notify_failed(f"new master prov[{new_master_idx}] not ta==1 (got ta={ta}); "
                           f"pre-seed lost, redistribution aborted")
            reset()
        return False
    except Exception as e:
        _rdlog_warn(f"consolidate check error: {e} — holding")
        return False


def _pool_tx_confirmed(R, kind: str, addr: str,
                       register_tx_confirm, get_tx_confirm_result, wait_for_block,
                       gap: bool = True) -> tuple:
    """Fire a liquidate/terminate on `addr`, wait for tx/executed + on-chain
    revert check, then (if gap) wait n+2 blocks and invalidate the capacity
    cache. Returns (ok, empty): empty=True iff the node had nothing to act on
    (already empty / not eligible) — a benign no-op, treated as success.
    """
    evt = register_tx_confirm(kind, addr)
    cmd = (f"pool liquidate --skip-confirmation --provisioner {addr}" if kind == "liquidate"
           else f"pool terminate --skip-confirmation --provisioner {addr}")
    r = R._cmd(cmd)
    if not r.get("ok"):
        err = r.get("stderr", "")[:300]
        if any(x in err.lower() for x in ("no stake", "does not exist",
                                          "nothing to", "not eligible")):
            return True, True
        _rdlog_err(f"{kind} prov@{addr[:12]}…: CLI failed: {err}")
        return False, False
    if not evt.wait(timeout=TX_CONFIRM_TIMEOUT):
        _rdlog_err(f"{kind} confirmation timeout")
        return False, False
    res = get_tx_confirm_result(kind, addr)
    if (res or {}).get("err"):
        _rdlog_err(f"{kind} REVERTED: {str(res['err'])[:200]}")
        return False, False
    if gap:
        blk = (res or {}).get("block_height", 0)
        if blk:
            if not wait_for_block(blk + 1, timeout=60):
                _rdlog_warn(f"  ↳ {kind} block-gap wait timeout — proceeding")
        else:
            time.sleep(22)
    try:
        from .assess import _invalidate_capacity_cache
        _invalidate_capacity_cache()
    except Exception:
        pass
    return True, False


def perform_consolidate(cur_epoch: int) -> bool:
    """The epoch-N+1 consolidate. Owns the ENTIRE rotation window (like heal's
    harvest): it liquidates BOTH rot_master and cur_master so the pool actually
    holds the excess, tops up new_master (ta==1) with excess + master (no slash),
    brings rot_slave up to `target` as the next rot_master, and re-seeds
    rot_active. The normal rotation body is skipped for this epoch.

    Returns True if it took over the window (caller MUST skip normal rotation),
    False if it aborted BEFORE touching any stake (caller runs normal rotation).

    Sequence (one tx in flight, n+2 gap between — mirrors run_harvest ordering):
      1. liquidate cur_master   (prov[0])       → pool   [PRE-COMMIT]
      2. terminate cur_master                            (free rewards + record)
      3. liquidate rot_master   (rot_active)    → pool   (frees the excess)
      4. terminate rot_master
      5. top-up new_master  (ta==1)  excess + master     (no slash; new master)
      6. top-up rot_slave   (ta==1)  → target            (next rot_master)
      7. re-seed rot_active          1k → ta=2
    """
    R = _rot_helpers()
    from .rues import register_tx_confirm, get_tx_confirm_result, wait_for_block

    with _rd_state_lock:
        new_master_idx = _rd_state.get("new_master_idx")
        cur_master_idx = _rd_state.get("cur_master_idx")
        target_dusk    = _rd_state.get("target_dusk", DEFAULT_TARGET_DUSK)

    st    = R._assess()
    nodes = st.get("by_idx", {})

    # ── Guard 1: no-master — new_master must be ta==1 ─────────────────────────
    if nodes.get(new_master_idx, {}).get("ta") != 1:
        _rdlog_err(f"consolidate ABORT: prov[{new_master_idx}] not ta==1 at execute time — "
                   f"master untouched.")
        _notify_failed(f"new master prov[{new_master_idx}] not ta==1 at execute — aborted")
        reset()
        return False

    # ── Guard 2: snatch-window time budget ───────────────────────────────────
    # The full sequence is up to 7 sequential txs; require a comfortable margin
    # above the snatch window so all of it lands before liquidated stake becomes
    # grabbable.
    try:
        from .rues import get_status as _rues_status
        blk_now  = (_rues_status() or {}).get("block_height", 0)
    except Exception:
        blk_now = R._current_block() if hasattr(R, "_current_block") else 0
    if blk_now:
        blk_left   = R.EPOCH_BLOCKS - (blk_now % R.EPOCH_BLOCKS)
        snatch_win = int(cfg("snatch_window") or 11)
        if blk_left <= snatch_win + 20:
            _rdlog_warn(f"consolidate deferred: blk_left={blk_left} too close to snatch "
                        f"({snatch_win}) for the full 7-tx sequence — retrying next window "
                        f"(master untouched)")
            return False

    cur_master_addr = R._addr(cur_master_idx, nodes)
    new_master_addr = R._addr(new_master_idx, nodes)
    master_stake    = nodes.get(cur_master_idx, {}).get("stake_dusk", 0.0)
    master_locked   = nodes.get(cur_master_idx, {}).get("locked_dusk", 0.0)
    rot_active_idx  = next((i for i in ROTATION_PAIR
                            if nodes.get(i, {}).get("status") == "active"
                            and nodes.get(i, {}).get("ta") == 0), None)
    rot_slave_idx   = next((i for i in ROTATION_PAIR
                            if nodes.get(i, {}).get("ta") == 1), None)

    # Need the rotation pair in the normal A:1 shape to run the full sequence.
    # If it isn't, don't touch the master — let normal rotation handle the odd
    # state this epoch and retry the consolidate next window.
    if rot_active_idx is None or rot_slave_idx is None:
        _rdlog_warn(f"consolidate: rotation pair not in expected shape "
                    f"(rot_active={rot_active_idx}, rot_slave={rot_slave_idx}) — deferring to "
                    f"normal rotation this epoch, master untouched")
        return False

    rot_active_addr = R._addr(rot_active_idx, nodes)
    rot_slave_addr  = R._addr(rot_slave_idx, nodes)
    rot_stake       = nodes.get(rot_active_idx, {}).get("stake_dusk", 0.0)
    rot_locked      = nodes.get(rot_active_idx, {}).get("locked_dusk", 0.0)
    rot_slave_stake = nodes.get(rot_slave_idx, {}).get("stake_dusk", 0.0)
    # excess counts the rot node's locked collateral too: liquidating rot_master
    # frees stake + locked to the pool, and everything above target must land on
    # the new master. Omitting rot_locked left it stranded in the pool where the
    # snatch window could take it (a ~10k loss on the first mainnet run).
    excess          = max(0.0, rot_stake + rot_locked - target_dusk)

    _rdlog_step(f"─── consolidate epoch {cur_epoch} ───")
    _rdlog_info(f"cur_master prov[{cur_master_idx}] stake={master_stake:.0f} locked={master_locked:.0f} | "
                f"new_master prov[{new_master_idx}] (ta=1) | rot_active prov[{rot_active_idx}] "
                f"stake={rot_stake:.0f} locked={rot_locked:.0f} | rot_slave prov[{rot_slave_idx}] | "
                f"target={target_dusk:.0f} | excess={excess:.0f}")

    # ── Step 1: liquidate cur_master → pool (PRE-COMMIT: abort keeps master) ──
    _rdlog_step(f"[1/7] liquidate cur_master prov[{cur_master_idx}] ({master_stake:.0f} DUSK)")
    ok, empty = _pool_tx_confirmed(R, "liquidate", cur_master_addr,
                                   register_tx_confirm, get_tx_confirm_result, wait_for_block)
    if empty:
        _rdlog_warn("[1/7] cur_master already empty — nothing to consolidate; aborting cleanly")
        _notify_failed("cur_master had no stake to liquidate — nothing to do")
        reset()
        return False
    if not ok:
        _rdlog_err("[1/7] liquidate cur_master failed/timeout — master untouched, aborting "
                   "(normal rotation will run)")
        _notify_failed("cur_master liquidate failed — aborted, master intact")
        reset()
        return False
    freed = master_stake + master_locked
    _rdlog_ok(f"[1/7] cur_master liquidated — {freed:.0f} DUSK freed to pool")

    # ══ COMMITTED: we own the window now; every exit below returns True ═══════
    rot_liquidated = False
    try:
        # ── Step 2: terminate cur_master (free rewards + clear record) ───────
        _rdlog_step(f"[2/7] terminate cur_master prov[{cur_master_idx}]")
        t_ok, _t_empty = _pool_tx_confirmed(R, "terminate", cur_master_addr,
                                            register_tx_confirm, get_tx_confirm_result, wait_for_block)
        if not t_ok:
            _rdlog_warn("[2/7] terminate cur_master failed — rewards not freed; continuing")

        # ── Step 3: liquidate rot_master (rot_active) → pool (frees excess) ──
        _rdlog_step(f"[3/7] liquidate rot_master prov[{rot_active_idx}] ({rot_stake:.0f} DUSK)")
        r_ok, r_empty = _pool_tx_confirmed(R, "liquidate", rot_active_addr,
                                           register_tx_confirm, get_tx_confirm_result, wait_for_block)
        rot_liquidated = r_ok and not r_empty
        if not rot_liquidated:
            _rdlog_warn(f"[3/7] rot_master liquidate {'empty' if r_empty else 'failed'} — the "
                        f"excess stays in the rotation pair this cycle; still recovering master stake")

        # ── Step 4: terminate rot_master (only if it had stake) ─────────────
        if rot_liquidated:
            _rdlog_step(f"[4/7] terminate rot_master prov[{rot_active_idx}]")
            rt_ok, _rt_empty = _pool_tx_confirmed(R, "terminate", rot_active_addr,
                                                  register_tx_confirm, get_tx_confirm_result, wait_for_block)
            if not rt_ok:
                _rdlog_warn("[4/7] terminate rot_master failed — re-seed in step 7 may be skipped")

        # ── Step 5: top-up new_master with excess + freed master (no slash) ──
        # If rot_master wasn't liquidated the excess isn't in the pool; the pool
        # clamp inside _stake_activate_clamped drops it automatically.
        topup_new = (excess if rot_liquidated else 0.0) + freed
        _rdlog_step(f"[5/7] top-up new_master prov[{new_master_idx}] with {topup_new:.0f} DUSK "
                    f"(excess {excess if rot_liquidated else 0:.0f} + master {freed:.0f})")
        new_ok = _stake_activate_clamped(R, new_master_idx, new_master_addr, topup_new,
                                         nodes, ctx="new_master", require_maturing=True)
        if not new_ok:
            _rdlog_err("[5/7] new_master top-up failed — entering failure-recovery ladder "
                       "(freed master stake must be re-staked before snatch)")
            if _recover_stranded_stake(R, topup_new, nodes, new_master_idx, rot_slave_idx):
                _notify_partial(cur_master_idx, new_master_idx)
            else:
                _rdlog_err("CRITICAL: could not re-stake freed master stake anywhere — "
                           "manual intervention required NOW before snatch window.")
                _notify_critical(topup_new)
            reset()
            return True
        _rdlog_ok(f"[5/7] new_master prov[{new_master_idx}] topped up — activates next boundary as "
                  f"the new master (~{topup_new:.0f} DUSK)")
        _repoint_master_hint(new_master_idx)

        # ── Steps 6 & 7: return the rotation pair to target (only if freed) ──
        if rot_liquidated:
            slave_topup = max(0.0, target_dusk - rot_slave_stake)
            if slave_topup >= 10.0:
                _rdlog_step(f"[6/7] top-up rot_slave prov[{rot_slave_idx}] to target "
                            f"({slave_topup:.0f} DUSK → {target_dusk:.0f}) — becomes next rot_master")
                if not _stake_activate_clamped(R, rot_slave_idx, rot_slave_addr, slave_topup,
                                               nodes, ctx="rot_slave", require_maturing=True):
                    _rdlog_warn("[6/7] rot_slave top-up failed — pool remainder left for the "
                                "state-check/deposit-race to allocate")
            else:
                _rdlog_info(f"[6/7] rot_slave already at/above target ({rot_slave_stake:.0f}) — skip")

            # rot_active was just liquidated+terminated → not active; require_maturing
            # is False here (the stale `nodes` still shows it active, which would
            # wrongly trip the anti-slash guard).
            _rdlog_step(f"[7/7] re-seed rot_active prov[{rot_active_idx}] with {SEED_DUSK:.0f} DUSK → ta=2")
            if not _stake_activate_clamped(R, rot_active_idx, rot_active_addr, SEED_DUSK,
                                           nodes, ctx="rot_active_reseed", require_maturing=False):
                _rdlog_warn("[7/7] rot_active re-seed failed — state check will retry seeding from pool")
        else:
            _rdlog_info("[6/7] rotation pair untouched (rot_master not liquidated) — "
                        "normal rotation cadence resumes next epoch")

        _rdlog_ok(f"─── consolidate epoch {cur_epoch} complete ✓ — master hopped "
                  f"prov[{cur_master_idx}]→prov[{new_master_idx}] ───")
        _notify_done(cur_master_idx, new_master_idx, topup_new, target_dusk)
        reset()
        return True

    except Exception as e:
        # Crash AFTER commit: freed stake must not be stranded through snatch.
        _rdlog_err(f"consolidate crashed after commit: {e} — attempting stranded-stake recovery")
        try:
            _recover_stranded_stake(R, (excess if rot_liquidated else 0.0) + freed,
                                    nodes, new_master_idx, rot_slave_idx)
        except Exception as e2:
            _rdlog_err(f"post-crash recovery also failed: {e2} — manual intervention needed")
            _notify_critical((excess if rot_liquidated else 0.0) + freed)
        reset()
        return True


# ── Capacity-clamped stake_activate ───────────────────────────────────────────
def _stake_activate_clamped(R, idx: int, addr: str, intended_dusk: float,
                            nodes: dict, ctx: str,
                            require_maturing: bool) -> bool:
    """Stake-activate `intended_dusk` onto prov[idx], clamped to active headroom
    and pool balance. If require_maturing, refuse unless the node is ta in (1,2)
    or inactive (never top up an ACTIVE node → would slash). Returns True on
    confirmed success.
    """
    from .wallet import WALLET_PATH
    from .rues import register_tx_confirm, get_tx_confirm_result
    from .pool import clamp_to_pool_balance

    status = nodes.get(idx, {}).get("status")
    ta     = nodes.get(idx, {}).get("ta")
    if require_maturing and status == "active":
        _rdlog_err(f"{ctx}: refuse to top up ACTIVE prov[{idx}] (would slash 10%) — skip")
        return False

    # Clamp to active headroom (maturing + slashed already counted in current).
    cap      = _capacity(R._pw())
    headroom = _active_headroom_dusk(cap)
    amt      = min(intended_dusk, headroom)
    if amt < intended_dusk:
        _rdlog_warn(f"{ctx}: clamped {intended_dusk:.0f} → {amt:.0f} DUSK "
                    f"(active headroom {headroom:.0f})")
    # Clamp to on-chain pool balance too.
    amt = clamp_to_pool_balance(amt, f"redistribute[{ctx}:prov{idx}]")
    if amt < SEED_DUSK:
        _rdlog_err(f"{ctx}: clamped to {amt:.4f} DUSK < {SEED_DUSK:.0f} — cannot stake")
        return False

    amt_lux = round(amt * 1e9)
    evt = register_tx_confirm("stake_activate", addr)
    r = R._cmd(
        f"pool stake-activate --skip-confirmation "
        f"--amount {amt_lux} --provisioner {addr} "
        f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{R._pw()}'")
    try:
        from .assess import _invalidate_capacity_cache
        _invalidate_capacity_cache()
    except Exception:
        pass

    if not r.get("ok"):
        _rdlog_err(f"{ctx}: stake-activate CLI failed: {r.get('stderr','')[:200]}")
        return False
    if not evt.wait(timeout=TX_CONFIRM_TIMEOUT):
        _rdlog_warn(f"{ctx}: stake-activate confirmation timeout — cannot confirm")
        return False
    res = get_tx_confirm_result("stake_activate", addr)
    if (res or {}).get("err"):
        _rdlog_err(f"{ctx}: stake-activate REVERTED: {str(res['err'])[:200]}")
        return False
    _rdlog_ok(f"{ctx}: staked {amt:.0f} DUSK → prov[{idx}] (confirmed)")
    return True


def _recover_stranded_stake(R, amount_dusk: float, nodes: dict,
                            failed_idx: int, rot_slave_idx: int | None) -> bool:
    """Re-stake stranded master stake. Ladder: retry failed target → rot_slave
    (ta==1) → any other ta==1 node → any node. Priority: OUT OF POOL before the
    snatch window, even if it means a 2-epoch maturation.
    """
    # 1. Retry the original target (transient errors) — cleanest, 1-epoch.
    failed_addr = R._addr(failed_idx, nodes)
    for attempt in range(1, TOPUP_RETRIES + 1):
        _rdlog_step(f"recovery: retry new_master prov[{failed_idx}] (attempt {attempt}/{TOPUP_RETRIES})")
        if _stake_activate_clamped(R, failed_idx, failed_addr, amount_dusk, nodes,
                                   ctx=f"recover-retry{attempt}", require_maturing=True):
            _repoint_master_hint(failed_idx)
            return True

    # 2. rot_slave (ta==1) — 1-epoch activation.
    if rot_slave_idx is not None and nodes.get(rot_slave_idx, {}).get("ta") == 1:
        _rdlog_step(f"recovery: staking to rot_slave prov[{rot_slave_idx}] (ta==1 → active next epoch)")
        addr = R._addr(rot_slave_idx, nodes)
        if _stake_activate_clamped(R, rot_slave_idx, addr, amount_dusk, nodes,
                                   ctx="recover-rotslave", require_maturing=True):
            return True

    # 3. any other ta==1 node.
    for i in NODE_INDICES:
        if i in (failed_idx, rot_slave_idx):
            continue
        if nodes.get(i, {}).get("ta") == 1:
            _rdlog_step(f"recovery: staking to ta==1 prov[{i}]")
            if _stake_activate_clamped(R, i, R._addr(i, nodes), amount_dusk, nodes,
                                       ctx=f"recover-ta1-{i}", require_maturing=True):
                return True

    # 4. any node that isn't active (inactive → 2 epochs, but safe).
    for i in NODE_INDICES:
        if nodes.get(i, {}).get("status") != "active":
            _rdlog_step(f"recovery: last resort — staking to prov[{i}] (status={nodes.get(i,{}).get('status')})")
            if _stake_activate_clamped(R, i, R._addr(i, nodes), amount_dusk, nodes,
                                       ctx=f"recover-any-{i}", require_maturing=True):
                return True

    return False


def _repoint_master_hint(new_idx: int) -> None:
    """Point cfg['master_idx'] at the new master so _master_idx tracks the hop.
    _master_idx auto-corrects from chain state anyway, but setting it avoids a
    transient wrong value until the next assess.
    """
    try:
        from .config import _cfg, _save_config
        _cfg["master_idx"] = new_idx
        _save_config(_cfg)
    except Exception as e:
        _rdlog_warn(f"could not repoint master_idx hint: {e}")


# ── Telegram notifications ────────────────────────────────────────────────────
def _tg(fn_name: str, *args) -> None:
    try:
        import lib.telegram as T
        fn = getattr(T, fn_name, None)
        if callable(fn):
            fn(*args)
    except Exception as e:
        _log(f"[redistribute] telegram '{fn_name}' failed: {e}")


def _notify_done(cur_idx, new_idx, topup_dusk, target_dusk) -> None:
    _tg("alert_info",
        f"✅ Redistribution complete.\n"
        f"New master: prov[{new_idx}] (~{topup_dusk:,.0f} DUSK, active next epoch)\n"
        f"Freed node: prov[{cur_idx}] (ready for next redistribution)\n"
        f"rot_master target: {target_dusk:,.0f} DUSK")


def _notify_partial(cur_idx, new_idx) -> None:
    _tg("alert_info",
        f"⚠️ Redistribution PARTIAL.\n"
        f"cur_master prov[{cur_idx}] was liquidated but top-up to prov[{new_idx}] failed.\n"
        f"Master stake was re-staked via the recovery ladder (see logs). "
        f"Manual role cleanup may be needed.")


def _notify_critical(amount_dusk) -> None:
    _tg("alert_info",
        f"🚨 Redistribution CRITICAL: {amount_dusk:,.0f} DUSK freed from master could NOT be "
        f"re-staked anywhere. Intervene manually NOW before the snatch window.")


def _notify_failed(reason: str) -> None:
    _tg("alert_info", f"❌ Manual redistribution failed: {reason}")
