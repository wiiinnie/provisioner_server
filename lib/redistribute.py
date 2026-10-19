"""
redistribute.py — Manual "consolidate-and-hop" stake redistribution.

PURPOSE
-------
Over time the rotation node (rot_master) accumulates far more stake than it
needs (e.g. 3.6M when only 1M is wanted for rotation). Under normal rotation
that whole amount is liquidated + re-staked every epoch, and it only ever
earns on the "rotation" role. The excess would be better parked on a large
"master-tier" node that never gets recycled.

This module moves the excess out of the rotation pair and onto one or more
empty master-tier nodes, WITHOUT the 10% top-up slash, by exploiting the same
mechanic rotation already relies on: topping up a ta==1 (maturing) node keeps
it maturing and it activates at the next epoch boundary — no slash, and the
big amount is only "in flight" for the rotation window, not two full epochs.

CONSOLIDATE-AND-HOP
-------------------
The master role hops between the two MASTER_PAIR nodes. Whichever is NOT the
current active master is the landing zone for the next redistribution. After
each redistribution the old master node is freed (inactive), ready to receive
the next one. This recycles forever.

  cur_master = the MASTER_PAIR node that is active now (untouched until the
               very last step)
  new_master = the other MASTER_PAIR node (inactive → pre-seeded → topped up)

[six_nodes] LANDING LIST
------------------------
A redistribution can land on MORE than the one hop node: every empty static
master (config.EXTRA_MASTERS, i.e. indices 4, 5, …) can be added to the
`landing` list with a percentage share. That is how the extra masters get
their stake at all (bootstrap): the pool stake is split by `shares` across all
landing nodes in ONE consolidate, because every landing node must be ta==1 in
the same epoch (the seeds all go out in the same rotation window). Day-to-day
the landing list is just the hop node at 100%, exactly the old behaviour; the
static masters are then never touched — unstakes front prov[0] first, so
they keep their stake until the operator arms a redistribution that includes
them again.

TWO-EPOCH FLOW
--------------
Epoch N   (rotation window): normal rotation runs with `n × 1000 DUSK` of the
          liquidated rot_master kept in the pool, PLUS pre-seed every landing
          node with 1000 DUSK (one wallet batch) → ta=2 this epoch.
Boundary  N→N+1: landing nodes ta 2 → 1.
Epoch N+1 (rotation window, before snatch): the consolidate OWNS the whole
          window (normal rotation is skipped) and runs three wallet batches:
    A. liquidate cur_master + liquidate rot_master      → pool (same block)
       (the contract wants terminate ≥ 2 blocks after liquidate: wait one block)
    B. terminate cur_master + terminate rot_master       (rewards → pool)
    C. stake-activate: every landing node by share (ta==1, no slash),
       rot_slave → target (next rot_master), rot_active 1k re-seed (ta=2)
       on a failed landing leg: fallback ladder (retry → rot_slave → any ta==1
       → any non-active node), stake must be out of the pool before snatch.
If the wallet refuses `--batch` for a command the batch degrades to the old
one-tx-at-a-time sequence automatically (same confirms, same guards).
Boundary  N+1→N+2: landing nodes active, cur_master freed, rot_slave active as
          rot_master at target. Master has hopped.

GUARDS
------
- Fronting-target defer: if any node we'd pre-seed sits at/ahead of the current
  master in the unstake fronting order AND we are the unstake target this epoch,
  the 1k pre-seed would be eaten. Defer to a later epoch. Reuses
  fronting.is_unstake_target_this_epoch().
- No-master: never liquidate cur_master until EVERY landing node is confirmed
  ta==1.
- Snatch window: all epoch-N+1 txs must complete before the snatch window
  (blk_left <= snatch_win) so liquidated stake is never exposed to the grab.
- Capacity: query operator capacity fresh before the activate batch and clamp
  to active headroom + pool balance. Never top up an ACTIVE node (would slash).

FAILURE RECOVERY (highest priority)
-----------------------------------
If cur_master is liquidated but a landing top-up fails, the master stake
must not be stranded in the pool through the snatch window. Recovery ladder:
    1. retry the failed landing node (ta==1) once or twice — cleanest
    2. rot_slave (ta==1)                          — active in 1 epoch
    3. any other ta==1 node                       — active in 1 epoch
    4. any node (even inactive → 2 epochs)        — safe, slower
Idle for 2 epochs beats losing master stake in the snatch race.

This module owns state file ~/.sozu_redistribute.json (JSON snapshot +
append log, same conventions as rotation). It is driven from rotation._run_rotation
via two hooks (preseed at epoch N, consolidate at epoch N+1) plus
preseed_reserve_dusk() for the rotation's pool allocation.
"""

import json
import os
import threading
import time

from .config import cfg, _log, MASTER_PAIR, ROTATION_PAIR, NODE_INDICES, EXTRA_MASTERS


# ── Constants ─────────────────────────────────────────────────────────────────
SEED_DUSK           = 1000.0
SEED_LUX            = round(SEED_DUSK * 1e9)
MIN_STAKE_DUSK      = 1000.0          # below this a node is treated as inactive
MIN_TOPUP_DUSK      = 10.0            # smallest top-up leg worth a tx
DEFAULT_TARGET_DUSK = 1_000_000.0     # default rot_master target
TX_CONFIRM_TIMEOUT  = 120             # seconds to wait for tx/executed
TOPUP_RETRIES       = 2               # retries on a failed landing top-up

STATE_FILE = os.path.expanduser("~/.sozu_redistribute.json")
LOG_PATH   = os.path.expanduser("~/.sozu_redistribute.log")

# ── States ────────────────────────────────────────────────────────────────────
IDLE       = "idle"        # nothing armed
ARMED      = "armed"       # operator armed; waiting for a clean epoch to pre-seed
PRESEEDED  = "preseeded"   # 1k pre-seed placed on every landing node; waiting for ta==1
DONE       = "done"        # transient — reset to IDLE after consolidate

_state_default = {
    "state":          IDLE,
    "target_dusk":    DEFAULT_TARGET_DUSK,
    "new_master_idx": None,   # the MASTER_PAIR landing node (hop target)
    "cur_master_idx": None,   # the active master at arm time (moves last)
    "landing":        [],     # [six_nodes] every landing node (hop node + static masters)
    "shares":         {},     # [six_nodes] {"idx": pct} — percentage of the moved stake per landing node
    "armed_epoch":    None,
    "preseed_epoch":  None,
    "status_msg":     "",
}

_rd_state      = dict(_state_default)
_rd_state_lock = threading.Lock()

_rd_log        = []
_rd_log_lock   = threading.Lock()


# ── Logging ─────────────────────────────────────────────────────────────────
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
                # JSON turns int keys into strings; normalise back.
                _rd_state["shares"]  = {int(k): float(v) for k, v in (_rd_state.get("shares") or {}).items()}
                _rd_state["landing"] = [int(i) for i in (_rd_state.get("landing") or [])]
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
    state["shares"] = {str(k): v for k, v in (state.get("shares") or {}).items()}
    return {**state, "log": log}


def reset() -> None:
    """Operator-initiated clear back to IDLE. Does NOT touch on-chain stake."""
    with _rd_state_lock:
        _rd_state.update(dict(_state_default))
        _rd_state["landing"] = []
        _rd_state["shares"]  = {}
        _rd_state["status_msg"] = "reset"
    _save_state()
    _rdlog("redistribute reset — back to IDLE")


def _landing_shares(landing: list, shares) -> dict:
    """Normalise `shares` (None | list of pct | {idx: pct}) for `landing` into
    {idx: pct} summing to 100. Raises ValueError on bad input."""
    if not landing:
        raise ValueError("landing list is empty")
    if shares is None or (isinstance(shares, (list, dict)) and len(shares) == 0):
        eq = 100.0 / len(landing)
        out = {i: eq for i in landing}
    elif isinstance(shares, dict):
        out = {}
        for i in landing:
            v = shares.get(i, shares.get(str(i)))
            if v is None:
                raise ValueError(f"no share for prov[{i}]")
            out[i] = float(v)
    else:
        if len(shares) != len(landing):
            raise ValueError("shares must have one entry per landing node")
        out = {i: float(v) for i, v in zip(landing, shares)}
    for i, v in out.items():
        if v <= 0.0:
            raise ValueError(f"share of prov[{i}] must be > 0 (got {v})")
    total = sum(out.values())
    if abs(total - 100.0) > 0.05:
        raise ValueError(f"shares must sum to 100 (got {total:.2f})")
    # absorb float drift on the hop node (or the first node)
    hop = next((i for i in landing if i in MASTER_PAIR), landing[0])
    out[hop] += 100.0 - total
    return out


def validate_landing(landing, cur_master_idx: int) -> tuple:
    """Check a landing list against the role split. Returns (landing, hop_idx)
    or raises ValueError. The list MUST contain the master-pair standby (the
    hop target) so the master role keeps hopping inside MASTER_PAIR; every
    other entry must be a static master (EXTRA_MASTERS)."""
    if landing is None or len(landing) == 0:
        hop = next((i for i in MASTER_PAIR if i != cur_master_idx), None)
        if hop is None:
            raise ValueError("cannot resolve the master-pair standby")
        return [hop], hop
    try:
        landing = [int(i) for i in landing]
    except Exception:
        raise ValueError("landing must be a list of node indices")
    if len(set(landing)) != len(landing):
        raise ValueError("landing contains duplicates")
    for i in landing:
        if i not in NODE_INDICES:
            raise ValueError(f"prov[{i}] is not a configured node ({NODE_INDICES})")
        if i in ROTATION_PAIR:
            raise ValueError(f"prov[{i}] is a rotation node — not a landing zone")
        if i == cur_master_idx:
            raise ValueError(f"prov[{i}] is the current master — it is the source, not a landing zone")
        if i not in MASTER_PAIR and i not in EXTRA_MASTERS:
            raise ValueError(f"prov[{i}] is neither in MASTER_PAIR nor a static master")
    hops = [i for i in landing if i in MASTER_PAIR]
    if len(hops) != 1:
        raise ValueError("landing must contain exactly one master-pair node (the standby the master hops to)")
    # keep a stable order: hop node first, then static masters ascending
    ordered = [hops[0]] + sorted(i for i in landing if i != hops[0])
    return ordered, hops[0]


def arm(target_dusk: float | None = None, landing=None, shares=None) -> dict:
    """Arm a redistribution. Executes across the next two suitable rotation
    windows. `target_dusk` is the desired rot_master stake after redistribution
    (default 1,000,000). [six_nodes] `landing` lists the nodes the moved stake
    lands on (default: the master-pair standby only) and `shares` their
    percentages (default: equal). Returns the resulting status dict.
    """
    if get_state() != IDLE:
        return {"ok": False, "error": f"redistribution already active (state={get_state()})"}

    target = float(target_dusk) if target_dusk else DEFAULT_TARGET_DUSK
    if target < SEED_DUSK:
        return {"ok": False, "error": f"target must be >= {SEED_DUSK:.0f} DUSK"}

    R = _rot_helpers()
    try:
        cur_master_idx = R._master_idx()
    except Exception as e:
        return {"ok": False, "error": f"cannot resolve current master: {e}"}
    if cur_master_idx not in MASTER_PAIR:
        return {"ok": False, "error": f"current master idx {cur_master_idx} not in MASTER_PAIR {MASTER_PAIR}"}
    try:
        landing, hop_idx = validate_landing(landing, cur_master_idx)
        share_map        = _landing_shares(landing, shares)
    except ValueError as ve:
        return {"ok": False, "error": str(ve)}

    share_str = ", ".join(f"prov[{i}] {share_map[i]:.1f}%" for i in landing)
    with _rd_state_lock:
        _rd_state.update({
            "state":          ARMED,
            "target_dusk":    target,
            "new_master_idx": hop_idx,
            "cur_master_idx": cur_master_idx,
            "landing":        list(landing),
            "shares":         dict(share_map),
            "armed_epoch":    None,
            "preseed_epoch":  None,
            "status_msg":     f"armed — target rot_master {target:,.0f} DUSK, landing {share_str}; "
                              f"waiting for next rotation window",
        })
    _save_state()
    _rdlog_step(f"armed — target rot_master {target:,.0f} DUSK | cur_master prov[{cur_master_idx}] | "
                f"landing {share_str}")
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
                     rot_active_idx: int, cur_master_idx: int,
                     landing: list | None = None, shares: dict | None = None) -> dict:
    """Compute the amounts a redistribution would move, for the preview UI and
    for the executor. All DUSK. [six_nodes] `landing`/`shares` split the moved
    stake; without them it all goes to the hop node (old behaviour).
    """
    rot_stake  = nodes.get(rot_active_idx, {}).get("stake_dusk", 0.0)
    rot_locked = nodes.get(rot_active_idx, {}).get("locked_dusk", 0.0)
    mas_stake  = nodes.get(cur_master_idx, {}).get("stake_dusk", 0.0)
    mas_locked = nodes.get(cur_master_idx, {}).get("locked_dusk", 0.0)
    # The consolidate liquidates BOTH cur_master and rot_master, freeing each
    # node's stake AND locked collateral to the pool; everything above `target`
    # lands on the landing nodes. Count locked on BOTH nodes: omitting the ROT
    # node's locked stranded it in the pool → snatch-exposed (a ~10k loss on the
    # first mainnet run); omitting the master's understated the size (~950k on
    # testnet). excess therefore includes rot_locked.
    excess     = max(0.0, rot_stake + rot_locked - target_dusk)
    new_master = excess + mas_stake + mas_locked
    per_node   = {}
    if landing:
        share_map = shares or _landing_shares(list(landing), None)
        for i in landing:
            pct = float(share_map.get(i, share_map.get(str(i), 0.0)))
            per_node[int(i)] = {
                "pct":    pct,
                "topup":  new_master * pct / 100.0,
                "status": nodes.get(int(i), {}).get("status"),
                "ta":     nodes.get(int(i), {}).get("ta"),
                "stake":  nodes.get(int(i), {}).get("stake_dusk", 0.0),
            }
    return {
        "rot_stake":       rot_stake,
        "rot_locked":      rot_locked,
        "master_stake":    mas_stake,
        "master_locked":   mas_locked,
        "target":          target_dusk,
        "excess":          excess,
        "new_master_size": new_master,
        "per_node":        {str(k): v for k, v in per_node.items()},
    }


# ── Wallet batches ────────────────────────────────────────────────────────────
_BATCH_FN = {"liquidate": "liquidate", "terminate": "terminate", "stake_activate": "stake_activate"}
_BENIGN_EMPTY = ("no stake", "does not exist", "nothing to", "not eligible", "should be active")


def _leg_cmd(leg: dict, batch: bool) -> str:
    from .wallet import WALLET_PATH
    R    = _rot_helpers()
    kind = leg["kind"]
    bflag = " --batch" if batch else ""
    if kind == "liquidate":
        return f"pool liquidate --skip-confirmation{bflag} --provisioner {leg['addr']}"
    if kind == "terminate":
        return f"pool terminate --skip-confirmation{bflag} --provisioner {leg['addr']}"
    if kind == "stake_activate":
        return (f"pool stake-activate --skip-confirmation{bflag} "
                f"--amount {int(leg['lux'])} --provisioner {leg['addr']} "
                f"--provisioner-wallet {WALLET_PATH} --provisioner-password '{R._pw()}'")
    raise ValueError(f"unknown leg kind {kind!r}")


def _cli_benign_empty(kind: str, stderr: str) -> bool:
    return kind in ("liquidate", "terminate") and any(x in (stderr or "").lower() for x in _BENIGN_EMPTY)


def _send_batch(R, legs: list, label: str, timeout: int = TX_CONFIRM_TIMEOUT) -> dict:
    """[six_nodes] Send `legs` as ONE wallet batch (same-block execution) and
    confirm each leg via tx/executed. legs: [{kind, addr, lux?, tag}]. Returns
    {tag: {ok, empty, confirmed, err, block_height}}.

    - A single leg is sent directly (no batch machinery).
    - If the wallet refuses a `--batch` queue command (unsupported flag, or
      any CLI failure while queueing) the queue is cleared and the legs go
      out ONE AT A TIME instead — the old, proven path. The block gap between
      a liquidate and its terminate is the caller's job (separate batches).
    - Confirmation: tx/executed per (fn, addr). Legs whose event does not
      arrive in time return confirmed=False and ok=False; callers verify by
      chain state where it matters (seeds) or treat it as failed (top-ups).
    """
    from .wallet import operator_cmd, tx_gate
    from .rues import register_tx_confirm, get_tx_confirm_result

    results = {}
    if not legs:
        return results
    pw = R._pw()

    def _register_all():
        return {leg["tag"]: register_tx_confirm(_BATCH_FN[leg["kind"]], leg["addr"]) for leg in legs}

    def _cli_fail(leg, stderr):
        results[leg["tag"]] = {"ok": False, "empty": False, "confirmed": False,
                               "err": f"CLI: {stderr[:200]}", "block_height": 0}

    def _collect(evts, sent_tags):
        deadline = time.time() + timeout
        for leg in legs:
            tag = leg["tag"]
            if tag not in sent_tags:
                continue
            evt = evts[tag]
            left = max(0.0, deadline - time.time())
            if not evt.wait(timeout=left):
                results[tag] = {"ok": False, "empty": False, "confirmed": False,
                                "err": "confirmation timeout", "block_height": 0}
                get_tx_confirm_result(_BATCH_FN[leg["kind"]], leg["addr"])   # drop the registration
                continue
            res = get_tx_confirm_result(_BATCH_FN[leg["kind"]], leg["addr"]) or {}
            err = res.get("err")
            blk = int(res.get("block_height", 0) or 0)
            if err:
                err_s = str(err)
                if leg["kind"] == "liquidate" and "should be active" in err_s.lower():
                    results[tag] = {"ok": True, "empty": True, "confirmed": True,
                                    "err": "", "block_height": blk}
                else:
                    results[tag] = {"ok": False, "empty": False, "confirmed": True,
                                    "err": f"REVERTED: {err_s[:200]}", "block_height": blk}
            else:
                results[tag] = {"ok": True, "empty": False, "confirmed": True,
                                "err": "", "block_height": blk}

    sent = set()
    if len(legs) == 1:
        evts = _register_all()
        leg  = legs[0]
        r    = operator_cmd(_leg_cmd(leg, batch=False), timeout=120, password=pw)
        if not r.get("ok"):
            stderr = r.get("stderr", "")
            if _cli_benign_empty(leg["kind"], stderr):
                results[leg["tag"]] = {"ok": True, "empty": True, "confirmed": False, "err": "", "block_height": 0}
            else:
                _cli_fail(leg, stderr)
            get_tx_confirm_result(_BATCH_FN[leg["kind"]], leg["addr"])
        else:
            sent.add(leg["tag"])
        _collect(evts, sent)
        _bump_caches()
        return results

    # ── real batch: queue every leg, propagate once, all under tx_gate ──
    queued_ok = True
    with tx_gate:
        evts = _register_all()
        for leg in legs:
            r = operator_cmd(_leg_cmd(leg, batch=True), timeout=120, password=pw)
            if not r.get("ok"):
                _rdlog_warn(f"{label}: batch queue of {leg['kind']} prov[{leg.get('idx','?')}] failed "
                            f"({r.get('stderr','')[:160]}) — clearing the queue, falling back to "
                            f"one tx at a time")
                operator_cmd("batch clear", timeout=30, password=pw)
                queued_ok = False
                break
        if queued_ok:
            r = operator_cmd("batch propagate --skip-confirmation --format json",
                             timeout=180, password=pw)
            if not r.get("ok"):
                _rdlog_warn(f"{label}: batch propagate failed ({r.get('stderr','')[:160]}) — "
                            f"clearing the queue, falling back to one tx at a time")
                operator_cmd("batch clear", timeout=30, password=pw)
                queued_ok = False
            else:
                sent = {leg["tag"] for leg in legs}
                _rdlog_info(f"{label}: batch of {len(legs)} txs propagated — waiting for confirmations")

        if not queued_ok:
            # drop the batch registrations, re-register per leg below
            for leg in legs:
                get_tx_confirm_result(_BATCH_FN[leg["kind"]], leg["addr"])

    if queued_ok:
        _collect(evts, sent)
        _bump_caches()
        return results

    # ── fallback: sequential sends, each confirmed on its own ──
    for leg in legs:
        sub = _send_batch(R, [leg], label, timeout=timeout)
        results.update(sub)
    return results


def _bump_caches() -> None:
    try:
        from .assess import _invalidate_capacity_cache, _invalidate_assess_cache
        _invalidate_capacity_cache()
        _invalidate_assess_cache()
    except Exception:
        pass


def _fresh_nodes(R) -> dict:
    _bump_caches()
    try:
        return R._assess().get("by_idx", {})
    except Exception as e:
        _rdlog_warn(f"fresh assess failed: {e}")
        return {}


# ── Phase hooks — called from rotation._run_rotation ──────────────────────────
def wants_preseed(cur_epoch: int) -> bool:
    """True if a redistribution is ARMED and this epoch's rotation window should
    also pre-seed the landing nodes. Applies the fronting-target defer guard.
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

        with _rd_state_lock:
            landing = list(_rd_state.get("landing") or [])
        try:
            landing, hop_idx = validate_landing(landing, cur_master_idx)
        except ValueError as ve:
            _rdlog_warn(f"preseed check: landing invalid for master prov[{cur_master_idx}] ({ve}) — deferring")
            return False

        # Every landing node must currently be inactive (a clean landing zone).
        for i in landing:
            st_i = nodes.get(i, {}).get("status")
            if st_i != "inactive":
                _rdlog_warn(f"preseed: landing prov[{i}] is '{st_i}', expected inactive — "
                            f"deferring (free it first)")
                return False

        # Fronting-target defer: only an issue if a landing node is exposed AND
        # we are the unstake target this epoch.
        exposed = [i for i in landing if _new_master_exposed_to_fronting(i, cur_master_idx)]
        if exposed:
            try:
                from .fronting import is_unstake_target_this_epoch
                if is_unstake_target_this_epoch(cur_epoch):
                    _rdlog_warn(f"preseed deferred: prov{exposed} sit ahead of the master in the "
                                f"fronting order and we are the unstake target this epoch — the 1k "
                                f"seed would be eaten. Waiting for a clear epoch.")
                    return False
            except Exception as _fe:
                _rdlog_warn(f"preseed: fronting-target check failed ({_fe}) — proceeding cautiously")

        # Record the resolved roles for the consolidate phase.
        with _rd_state_lock:
            _rd_state["cur_master_idx"] = cur_master_idx
            _rd_state["new_master_idx"] = hop_idx
            _rd_state["landing"]        = list(landing)
        return True
    except Exception as e:
        _rdlog_warn(f"preseed check error: {e} — deferring")
        return False


def preseed_reserve_dusk(cur_epoch: int) -> float:
    """[six_nodes] DUSK the rotation must leave in the pool for this window's
    pre-seed: one SEED_DUSK per landing node, 0 when no pre-seed is pending."""
    if not wants_preseed(cur_epoch):
        return 0.0
    with _rd_state_lock:
        n = len(_rd_state.get("landing") or [])
    return SEED_DUSK * max(1, n)


def perform_preseed(cur_epoch: int) -> None:
    """Seed every landing node with 1000 DUSK (one wallet batch) so they start
    maturing. Called during epoch N's rotation window after the normal
    rotation steps.
    """
    R = _rot_helpers()

    with _rd_state_lock:
        landing        = list(_rd_state.get("landing") or [])
        cur_master_idx = _rd_state.get("cur_master_idx")
        shares         = dict(_rd_state.get("shares") or {})

    if not landing:
        _rdlog_err("perform_preseed: landing list empty — aborting")
        return

    st    = R._assess()
    nodes = st.get("by_idx", {})
    legs  = []
    for i in landing:
        addr = R._addr(i, nodes)
        if not addr:
            _rdlog_err(f"perform_preseed: no address for prov[{i}] — aborting")
            return
        legs.append({"kind": "stake_activate", "addr": addr, "lux": SEED_LUX, "idx": i, "tag": i})

    need = SEED_DUSK * len(legs)

    # Capacity guard — seeding adds SEED_DUSK per node to active_current (maturing counts).
    cap = _capacity(R._pw())
    if _active_headroom_dusk(cap) < need:
        _rdlog_err(f"perform_preseed: only {_active_headroom_dusk(cap):.0f} DUSK active headroom "
                   f"< {need:.0f} seed(s) — cannot pre-seed, staying ARMED")
        return

    # Pool guard — the seeds are drawn from the pool and revert on-chain when the
    # pool holds less ("should not exceed the pending stake"). The rotation
    # reserves them ([redistribute_preseed_reserve]); a user unstake served from
    # the pool in between can still eat them. Defer instead of burning txs.
    try:
        from .pool import _query_pool_balance_dusk
        pool_dusk = _query_pool_balance_dusk(strict=True)
    except Exception as _pe:
        _rdlog_warn(f"perform_preseed: pool balance query failed ({_pe}) — sending the seeds unchecked")
        pool_dusk = None
    if pool_dusk is not None and pool_dusk < need:
        msg = (f"pre-seed deferred: pool {pool_dusk:.4f} DUSK < {need:.0f} DUSK for {len(legs)} seed(s) — "
               f"staying ARMED, retries next rotation window")
        _rdlog_warn(msg)
        with _rd_state_lock:
            _rd_state["status_msg"] = msg
        _save_state()
        _tg("alert_info", f"⚠️ Redistribution pre-seed deferred: pool {pool_dusk:,.0f} DUSK "
                          f"< {need:,.0f} DUSK for {len(legs)} seed(s). Still ARMED, retries next window.")
        return

    _rdlog_step(f"pre-seed prov{landing} with {SEED_DUSK:.0f} DUSK each → ta=2 "
                f"(cur master prov[{cur_master_idx}], {'one batch' if len(legs) > 1 else 'single tx'})")
    results = _send_batch(R, legs, label="pre-seed")

    seeded, unknown, failed = [], [], []
    for i in landing:
        r = results.get(i) or {}
        if r.get("ok") and not r.get("empty"):
            seeded.append(i)
        elif not r.get("confirmed") and not str(r.get("err", "")).startswith("CLI"):
            unknown.append(i)
        else:
            failed.append(i)
            _rdlog_err(f"pre-seed prov[{i}] FAILED: {r.get('err', 'unknown')}")

    if unknown:
        # No event in time — the chain decides. Re-assess and keep what landed.
        _rdlog_warn(f"pre-seed prov{unknown}: no confirmation in time — verifying on-chain state")
        fresh = _fresh_nodes(R)
        for i in unknown:
            if fresh.get(i, {}).get("status") in ("seeded", "maturing"):
                seeded.append(i)
            else:
                failed.append(i)
                _rdlog_err(f"pre-seed prov[{i}]: not seeded on-chain after timeout")

    if not seeded:
        _rdlog_err("pre-seed: no landing node seeded — staying ARMED, will retry next window")
        _tg("alert_info", f"❌ Redistribution pre-seed failed on every landing node prov{landing}. "
                          f"Still ARMED, retries next window.")
        return

    seeded = sorted(seeded, key=landing.index)
    if failed:
        # Proceed with what landed: an unseeded node is simply left out and its
        # share is spread over the seeded ones. (A seeded node that is later NOT
        # consolidated would be an orphan 1k master — cleaning that is manual.)
        dropped = {i: shares.get(i, 0.0) for i in failed}
        rest    = sum(shares.get(i, 0.0) for i in seeded) or 100.0
        shares  = {i: shares.get(i, 0.0) * 100.0 / rest for i in seeded}
        hop_ok  = any(i in MASTER_PAIR for i in seeded)
        _rdlog_warn(f"pre-seed PARTIAL: seeded prov{seeded}, failed prov{failed} — consolidate will "
                    f"split over the seeded nodes only (shares renormalised: "
                    f"{', '.join(f'prov[{i}] {p:.1f}%' for i, p in shares.items())})"
                    + ("" if hop_ok else " — NOTE: the master-pair hop node did not seed; the master "
                                        "stake will land on static masters only"))
        _tg("alert_info", f"⚠️ Redistribution pre-seed PARTIAL: seeded prov{seeded}, failed prov{failed} "
                          f"(dropped shares {', '.join(f'prov[{i}] {p:.1f}%' for i, p in dropped.items())}). "
                          f"Consolidate next window splits over the seeded nodes only.")
    else:
        _rdlog_ok(f"pre-seed confirmed — prov{seeded} now ta=2")

    with _rd_state_lock:
        _rd_state["state"]         = PRESEEDED
        _rd_state["landing"]       = list(seeded)
        _rd_state["shares"]        = dict(shares)
        _rd_state["new_master_idx"] = next((i for i in seeded if i in MASTER_PAIR), _rd_state.get("new_master_idx"))
        _rd_state["preseed_epoch"] = cur_epoch
        _rd_state["armed_epoch"]   = _rd_state.get("armed_epoch") or cur_epoch
        _rd_state["status_msg"]    = (f"pre-seeded prov{seeded}; consolidate next rotation window "
                                      f"once all reach ta==1")
    _save_state()


def wants_consolidate(cur_epoch: int) -> bool:
    """True if we are PRESEEDED and EVERY landing node has reached ta==1 —
    meaning the consolidate can run in THIS rotation window. If a landing
    node is still ta==2 (should be ta==1 exactly one epoch after pre-seed), we
    do NOT touch the master; we hold and re-check next epoch. Any other state
    means the seed was lost → abort, master untouched.
    """
    if get_state() != PRESEEDED:
        return False

    R = _rot_helpers()
    try:
        st    = R._assess()
        nodes = st.get("by_idx", {})
        with _rd_state_lock:
            landing = list(_rd_state.get("landing") or [])
        tas  = {i: nodes.get(i, {}).get("ta") for i in landing}
        if landing and all(t == 1 for t in tas.values()):
            return True
        lost = [i for i, t in tas.items() if t not in (1, 2)]
        if lost:
            _rdlog_err(f"consolidate ABORT: landing ta={tas} — prov{lost} not maturing, "
                       f"pre-seed was lost. Master untouched. Resetting.")
            _notify_failed(f"landing nodes prov{lost} not ta==1 (ta={tas}); "
                           f"pre-seed lost, redistribution aborted")
            reset()
        else:
            _rdlog_warn(f"consolidate hold: landing ta={tas} — waiting one more epoch for ta==1")
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
    (Kept for the recovery paths; the consolidate itself uses _send_batch.)
    """
    evt = register_tx_confirm(kind, addr)
    cmd = (f"pool liquidate --skip-confirmation --provisioner {addr}" if kind == "liquidate"
           else f"pool terminate --skip-confirmation --provisioner {addr}")
    r = R._cmd(cmd)
    if not r.get("ok"):
        err = r.get("stderr", "")[:300]
        if any(x in err.lower() for x in _BENIGN_EMPTY):
            return True, True
        _rdlog_err(f"{kind} prov@{addr[:12]}…: CLI failed: {err}")
        return False, False
    if not evt.wait(timeout=TX_CONFIRM_TIMEOUT):
        _rdlog_err(f"{kind} confirmation timeout")
        return False, False
    res = get_tx_confirm_result(kind, addr)
    if (res or {}).get("err"):
        err = str(res["err"])
        if kind == "liquidate" and "should be active" in err.lower():
            # [pool] "The provisioner should be active" — nothing staked on
            # this node (testnet 2026-10-02 03:43: cur_master prov[0] was
            # already empty). Benign no-op, not a failure.
            _rdlog_warn(f"{kind} prov@{addr[:12]}…: nothing staked ({err[:80]}) — treating as empty")
            return True, True
        _rdlog_err(f"{kind} REVERTED: {err[:200]}")
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
    """The epoch-N+1 consolidate. Owns the ENTIRE rotation window (normal
    rotation is skipped): it liquidates BOTH rot_master and cur_master so the pool
    actually holds the excess, tops up every landing node (ta==1) by share with
    excess + master (no slash), brings rot_slave up to `target` as the next
    rot_master, and re-seeds rot_active.

    Returns True if it took over the window (caller MUST skip normal rotation),
    False if it aborted BEFORE touching any stake (caller runs normal rotation).

    Sequence ([six_nodes] three wallet batches, same-block execution each):
      A. liquidate cur_master + liquidate rot_master     → pool   [PRE-COMMIT]
         wait one block (terminate must land ≥ 2 blocks after liquidate)
      B. terminate cur_master + terminate rot_master      (free rewards + record)
      C. top-up landing nodes (ta==1) by share, excess + master   (no slash)
         top-up rot_slave   (ta==1)  → target                      (next rot_master)
         re-seed rot_active          1k → ta=2
    """
    R = _rot_helpers()
    from .rues import get_status as _rues_status, wait_for_block

    with _rd_state_lock:
        landing        = list(_rd_state.get("landing") or [])
        shares         = dict(_rd_state.get("shares") or {})
        new_master_idx = _rd_state.get("new_master_idx")
        cur_master_idx = _rd_state.get("cur_master_idx")
        target_dusk    = _rd_state.get("target_dusk", DEFAULT_TARGET_DUSK)

    st    = R._assess()
    nodes = st.get("by_idx", {})
    # Snapshot the operator's total on-node stake (stake + locked) BEFORE the
    # consolidate so the completion alert can report the delta. It should end
    # >= 0 (conserved, or higher if idle pool stake was swept in); a drop means
    # stake leaked off-node — stranded in the pool or taken by the snatch window.
    stake_sum_before = _committed_sum(nodes)

    # ── Guard 1: no-master — every landing node must be ta==1 ─────────────────
    not_ready = [i for i in landing if nodes.get(i, {}).get("ta") != 1]
    if not landing or not_ready:
        _rdlog_err(f"consolidate ABORT: landing prov{not_ready or landing} not ta==1 at execute time — "
                   f"master untouched.")
        _notify_failed(f"landing nodes prov{not_ready or landing} not ta==1 at execute — aborted")
        reset()
        return False

    # ── Guard 2: snatch-window time budget ───────────────────────────────────
    # Three batches plus confirmations; require a comfortable margin above the
    # snatch window so all of it lands before liquidated stake becomes grabbable.
    try:
        blk_now  = (_rues_status() or {}).get("block_height", 0)
    except Exception:
        blk_now = R._current_block() if hasattr(R, "_current_block") else 0
    if blk_now:
        blk_left   = R.EPOCH_BLOCKS - (blk_now % R.EPOCH_BLOCKS)
        snatch_win = int(cfg("snatch_window") or 11)
        if blk_left <= snatch_win + 20:
            _rdlog_warn(f"consolidate deferred: blk_left={blk_left} too close to snatch "
                        f"({snatch_win}) for the full sequence — retrying next window "
                        f"(master untouched)")
            return False

    cur_master_addr = R._addr(cur_master_idx, nodes)
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
    # the landing nodes. Omitting rot_locked left it stranded in the pool where
    # the snatch window could take it (a ~10k loss on the first mainnet run).
    excess          = max(0.0, rot_stake + rot_locked - target_dusk)
    share_str       = ", ".join(f"prov[{i}] {shares.get(i, 0.0):.1f}%" for i in landing)

    _rdlog_step(f"─── consolidate epoch {cur_epoch} ───")
    _rdlog_info(f"cur_master prov[{cur_master_idx}] stake={master_stake:.0f} locked={master_locked:.0f} | "
                f"landing {share_str} (all ta=1) | rot_active prov[{rot_active_idx}] "
                f"stake={rot_stake:.0f} locked={rot_locked:.0f} | rot_slave prov[{rot_slave_idx}] | "
                f"target={target_dusk:.0f} | excess={excess:.0f}")

    # ── Batch A: liquidate cur_master + rot_master → pool (PRE-COMMIT) ────────
    # [empty_master] An empty cur_master (already unstaked / hopped away) is no
    # reason to abort: the excess on rot_master still wants to move onto the
    # landing nodes. Skip the master legs and consolidate the rotation side only.
    master_empty = master_stake <= 0.0 and master_locked <= 0.0
    if master_empty and excess < SEED_DUSK:
        _rdlog_warn(f"[A] cur_master prov[{cur_master_idx}] empty and rot_master excess "
                    f"{excess:.0f} < {SEED_DUSK:.0f} DUSK — nothing to consolidate; aborting cleanly")
        _notify_failed("cur_master empty and no rot_master excess — nothing to do")
        reset()
        return False
    if master_empty:
        _rdlog_warn(f"[A] cur_master prov[{cur_master_idx}] has no stake — skipping master "
                    f"liquidate/terminate, moving the rot_master excess ({excess:.0f} DUSK) only")

    legs_a = []
    if not master_empty:
        legs_a.append({"kind": "liquidate", "addr": cur_master_addr, "idx": cur_master_idx, "tag": "master"})
    legs_a.append({"kind": "liquidate", "addr": rot_active_addr, "idx": rot_active_idx, "tag": "rot"})
    _rdlog_step(f"[A] liquidate {'cur_master prov[%d] (%.0f DUSK) + ' % (cur_master_idx, master_stake) if not master_empty else ''}"
                f"rot_master prov[{rot_active_idx}] ({rot_stake:.0f} DUSK)")
    res_a = _send_batch(R, legs_a, label="[A] liquidate")

    ra_m = res_a.get("master", {"ok": True, "empty": True, "confirmed": True, "block_height": 0})
    ra_r = res_a.get("rot",    {})
    master_liquidated = (not master_empty) and ra_m.get("ok") and not ra_m.get("empty")
    rot_liquidated    = bool(ra_r.get("ok") and not ra_r.get("empty"))
    if not master_empty and ra_m.get("empty"):
        _rdlog_warn("[A] cur_master reverted as empty (assess was stale) — continuing without master stake")
        master_empty = True
    if not master_empty and not master_liquidated:
        _rdlog_err(f"[A] liquidate cur_master failed: {ra_m.get('err')}")
    if not rot_liquidated:
        _rdlog_warn(f"[A] rot_master liquidate {'empty' if ra_r.get('empty') else 'failed: ' + str(ra_r.get('err'))}")

    freed = (master_stake + master_locked) if master_liquidated else 0.0
    if not master_liquidated and not rot_liquidated:
        # Nothing has left any node → clean pre-commit abort.
        _rdlog_err("[A] nothing liquidated — aborting (normal rotation will run), master intact")
        _notify_failed("liquidate batch failed — aborted, nothing moved")
        reset()
        return False
    if master_liquidated:
        _rdlog_ok(f"[A] cur_master liquidated — {freed:.0f} DUSK freed to pool")
    if rot_liquidated:
        _rdlog_ok(f"[A] rot_master liquidated — {rot_stake + rot_locked:.0f} DUSK freed to pool")
    if not master_empty and not master_liquidated:
        # rot liquidated, master still active: the hop cannot happen this cycle.
        # The excess still goes to the landing nodes (they are ta==1 and empty
        # otherwise → orphan seeds). The master simply keeps its role.
        _rdlog_warn("[A] master NOT liquidated but rot_master is — moving the excess only; "
                    "the master keeps its role this cycle")

    # ══ COMMITTED once stake left a node. Every exit after this returns True.
    try:
        # block gap: terminate must be included ≥ 2 blocks after the liquidate
        blk_liq = max(int(ra_m.get("block_height", 0) or 0), int(ra_r.get("block_height", 0) or 0))
        if blk_liq:
            _rdlog_step(f"  ↳ liquidate executed at block {blk_liq}; waiting for block {blk_liq + 1} (n+2 safety)")
            if not wait_for_block(blk_liq + 1, timeout=60):
                _rdlog_warn("  ↳ block-gap wait timeout — proceeding")
        else:
            time.sleep(22)

        # ── Batch B: terminate (rewards → pool, clear records) ───────────────
        legs_b = []
        if master_liquidated:
            legs_b.append({"kind": "terminate", "addr": cur_master_addr, "idx": cur_master_idx, "tag": "master"})
        if rot_liquidated:
            legs_b.append({"kind": "terminate", "addr": rot_active_addr, "idx": rot_active_idx, "tag": "rot"})
        _rdlog_step(f"[B] terminate {' + '.join('prov[%d]' % l['idx'] for l in legs_b)}")
        res_b = _send_batch(R, legs_b, label="[B] terminate")
        for tag, leg in (("master", cur_master_idx), ("rot", rot_active_idx)):
            r = res_b.get(tag)
            if r is None:
                continue
            if r.get("ok"):
                _rdlog_ok(f"[B] terminate prov[{leg}] confirmed")
            else:
                _rdlog_warn(f"[B] terminate prov[{leg}] failed ({r.get('err')}) — rewards not freed; continuing")

        # ── Batch C: activates ───────────────────────────────────────────────
        # Budget = what the pool holds, capped by active headroom (fresh) — but
        # never below the stake we just liquidated: re-staking that is
        # capacity-neutral and always fits, so a stale capacity read must not
        # strand it in the pool (same rule as the rotation's [seed_capacity_reserve]).
        # Only rewards / idle pool stake grow active_current and are subject to
        # the headroom. Priority: rot_active seed, landing nodes, rot_slave.
        recycled = freed + ((rot_stake + rot_locked) if rot_liquidated else 0.0)
        cap      = _capacity(R._pw())
        headroom = _active_headroom_dusk(cap)
        try:
            from .pool import _query_pool_balance_dusk
            pool_now = _query_pool_balance_dusk(strict=True)
        except Exception as _pe:
            _rdlog_warn(f"[C] pool balance query failed ({_pe}) — using the computed amounts")
            pool_now = None
        budget = max(headroom, recycled)
        if pool_now is not None:
            budget = min(budget, pool_now)
        _rdlog_info(f"[C] budget: headroom {headroom:.0f}, recycled {recycled:.0f}, pool "
                    f"{'n/a' if pool_now is None else f'{pool_now:.0f}'} DUSK → {budget:.0f} DUSK")

        seed_amt = 0.0
        if rot_liquidated:
            seed_amt = min(SEED_DUSK, budget)
            if seed_amt < SEED_DUSK:
                _rdlog_warn(f"[C] budget {budget:.0f} < {SEED_DUSK:.0f} DUSK — rot_active re-seed skipped; "
                            f"state check retries from pool")
                seed_amt = 0.0
            budget -= seed_amt

        landing_total = (excess if rot_liquidated else 0.0) + freed
        landing_alloc = min(landing_total, budget)
        if landing_alloc < landing_total:
            _rdlog_warn(f"[C] landing total clamped {landing_total:.0f} → {landing_alloc:.0f} DUSK "
                        f"(budget after seed {budget:.0f})")
        budget -= landing_alloc

        slave_topup = 0.0
        if rot_liquidated:
            slave_topup = min(max(0.0, target_dusk - rot_slave_stake), budget)
            budget -= slave_topup

        # split the landing allocation by share; the hop node absorbs rounding
        amounts = {}
        for i in landing:
            amounts[i] = landing_alloc * float(shares.get(i, 0.0)) / 100.0
        hop_idx = next((i for i in landing if i in MASTER_PAIR), landing[0])
        amounts[hop_idx] += landing_alloc - sum(amounts.values())

        legs_c = []
        if seed_amt >= SEED_DUSK:
            legs_c.append({"kind": "stake_activate", "addr": rot_active_addr, "lux": round(seed_amt * 1e9),
                           "idx": rot_active_idx, "tag": "seed"})
        for i in landing:
            if amounts[i] < MIN_TOPUP_DUSK:
                _rdlog_warn(f"[C] landing prov[{i}] share {amounts[i]:.2f} DUSK < {MIN_TOPUP_DUSK:.0f} — leg skipped")
                continue
            if nodes.get(i, {}).get("status") == "active":
                _rdlog_err(f"[C] refuse to top up ACTIVE prov[{i}] (would slash 10%) — leg skipped")
                continue
            legs_c.append({"kind": "stake_activate", "addr": R._addr(i, nodes), "lux": round(amounts[i] * 1e9),
                           "idx": i, "tag": f"land{i}"})
        if slave_topup >= MIN_TOPUP_DUSK:
            legs_c.append({"kind": "stake_activate", "addr": rot_slave_addr, "lux": round(slave_topup * 1e9),
                           "idx": rot_slave_idx, "tag": "slave"})
        elif rot_liquidated:
            _rdlog_info(f"[C] rot_slave already at/above target ({rot_slave_stake:.0f}) — no top-up leg")

        _rdlog_step("[C] stake-activate: " + " + ".join(
            f"prov[{l['idx']}] {l['lux']/1e9:,.0f}" for l in legs_c) + " DUSK")
        res_c = _send_batch(R, legs_c, label="[C] activate")

        landed_total = 0.0
        failed_landing = []
        for i in landing:
            r = res_c.get(f"land{i}")
            if r is None:
                continue
            if r.get("ok"):
                landed_total += amounts[i]
                _rdlog_ok(f"[C] prov[{i}] topped up {amounts[i]:,.0f} DUSK (confirmed) — active next boundary")
            else:
                failed_landing.append(i)
                _rdlog_err(f"[C] prov[{i}] top-up FAILED: {r.get('err')}")
        r_seed  = res_c.get("seed")
        r_slave = res_c.get("slave")
        if r_seed is not None:
            if r_seed.get("ok"):
                _rdlog_ok(f"[C] rot_active prov[{rot_active_idx}] re-seeded {SEED_DUSK:.0f} DUSK → ta=2")
            else:
                _rdlog_warn(f"[C] rot_active re-seed failed ({r_seed.get('err')}) — state check retries from pool")
        if r_slave is not None:
            if r_slave.get("ok"):
                _rdlog_ok(f"[C] rot_slave prov[{rot_slave_idx}] topped up {slave_topup:,.0f} DUSK → next rot_master")
            else:
                _rdlog_warn(f"[C] rot_slave top-up failed ({r_slave.get('err')}) — pool remainder left for "
                            f"the state-check/deposit-race to allocate")

        partial = False
        if failed_landing:
            stranded = sum(amounts[i] for i in failed_landing)
            _rdlog_err(f"[C] {stranded:,.0f} DUSK for prov{failed_landing} not staked — entering the "
                       f"failure-recovery ladder (must be out of the pool before snatch)")
            fresh = _fresh_nodes(R) or nodes
            if _recover_stranded_stake(R, stranded, fresh, failed_landing[0], rot_slave_idx):
                partial = True
                _notify_partial(cur_master_idx, failed_landing)
            else:
                _rdlog_err("CRITICAL: could not re-stake the stranded stake anywhere — "
                           "manual intervention required NOW before snatch window.")
                _notify_critical(stranded)
                reset()
                return True

        if master_liquidated and hop_idx not in failed_landing and (res_c.get(f"land{hop_idx}") or {}).get("ok"):
            _repoint_master_hint(hop_idx)

        _rdlog_ok(f"─── consolidate epoch {cur_epoch} complete{' (PARTIAL)' if partial else ' ✓'} — "
                  f"master prov[{cur_master_idx}] → landing {share_str} ───")
        try:
            stake_sum_after = _committed_sum(R._assess().get("by_idx", {}))
        except Exception as _se:
            _rdlog_warn(f"post-consolidate stake-sum assess failed ({_se}) — reporting no delta")
            stake_sum_after = stake_sum_before
        _rdlog_info(f"stake sum: {stake_sum_before:,.0f} → {stake_sum_after:,.0f} DUSK "
                    f"(Δ {stake_sum_after - stake_sum_before:+,.0f})")
        if not partial:
            _notify_done(cur_master_idx, landing, amounts, target_dusk,
                         stake_sum_before, stake_sum_after)
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
        _rdlog_step(f"recovery: retry landing prov[{failed_idx}] (attempt {attempt}/{TOPUP_RETRIES})")
        if _stake_activate_clamped(R, failed_idx, failed_addr, amount_dusk, nodes,
                                   ctx=f"recover-retry{attempt}", require_maturing=True):
            if failed_idx in MASTER_PAIR:
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


def _committed_sum(nodes: dict) -> float:
    """Total operator stake committed ON NODES = Σ(stake + locked) over all
    provisioners. A redistribution should conserve this (or grow it, if idle
    pool stake was swept in). A DROP means stake left the nodes — stranded in
    the pool (recoverable) or taken by the snatch window."""
    return sum((n.get("stake_dusk", 0.0) + n.get("locked_dusk", 0.0))
               for n in nodes.values())


def _notify_done(cur_idx, landing, amounts, target_dusk,
                 sum_before=None, sum_after=None) -> None:
    lines = ["✅ Redistribution complete."]
    for i in landing:
        role = "new master" if i in MASTER_PAIR else "static master"
        lines.append(f"{role}: prov[{i}] (+{amounts.get(i, 0.0):,.0f} DUSK, active next epoch)")
    lines.append(f"Freed node: prov[{cur_idx}] (ready for next redistribution)")
    lines.append(f"rot_master target: {target_dusk:,.0f} DUSK")
    if sum_before is not None and sum_after is not None:
        delta = sum_after - sum_before
        if delta < -1.0:
            lines.append(f"⚠️ Stake sum {sum_before:,.0f} → {sum_after:,.0f} DUSK "
                         f"(Δ {delta:+,.0f}) — DECREASED; stake left the nodes, "
                         f"check the pool (recoverable) or it was snatched")
        else:
            lines.append(f"Stake sum {sum_before:,.0f} → {sum_after:,.0f} DUSK "
                         f"(Δ {delta:+,.0f}) ✅ conserved")
    _tg("alert_info", "\n".join(lines))


def _notify_partial(cur_idx, failed_landing) -> None:
    _tg("alert_info",
        f"⚠️ Redistribution PARTIAL.\n"
        f"cur_master prov[{cur_idx}] was liquidated but the top-up of prov{failed_landing} failed.\n"
        f"That stake was re-staked via the recovery ladder (see logs). "
        f"Manual role cleanup may be needed.")


def _notify_critical(amount_dusk) -> None:
    _tg("alert_info",
        f"🚨 Redistribution CRITICAL: {amount_dusk:,.0f} DUSK freed from master could NOT be "
        f"re-staked anywhere. Intervene manually NOW before the snatch window.")


def _notify_failed(reason: str) -> None:
    _tg("alert_info", f"❌ Manual redistribution failed: {reason}")
