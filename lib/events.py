"""
events.py — RUES event action engine.

Allocation rules per window:
  Regular / Rotation:
    deposit           → allocate   (emitted by sozu_stake)
    donate            → allocate   (emitted by sozu_airdrop)
    reward/recycle    → allocate
    reward/terminate  → allocate ONLY if own provisioner was terminated

  Snatch:
    deposit           → allocate
    donate            → allocate
    reward/recycle    → allocate
    reward/terminate  → always allocate

No rot_slave (rotation pair A:1 M:0 I:1), deposit ≥ 1000 DUSK:
    one batch: seed the inactive rot node 1000 + rest → rot_active (same block)

3-step deposit race flow:
  Step 1 (deposit_received):  event arrived, stake-activate fired
  Step 2 (tx_confirmed/failed): tx/executed result — win/loss decided here
  Step 3 (activate_confirmed): activate contract event — stake is live on-chain

⚠ TODO:
  - Target selection incomplete: multi-node split beyond the seed split,
    configurable override
  - Threshold check bypassed (if False) — re-enable when stable
  - Deposit ownership filtering (depositor address) — "we need this later"
  - block_height None for contract events — wire up block cache backfill
"""

import threading
import time
from collections import deque
from datetime import datetime

from .config import _log, cfg, ROTATION_PAIR

EPOCH_BLOCKS: int = 2160

_deposit_log:      deque = deque(maxlen=200)
_deposit_log_lock         = threading.Lock()

_race_wins:   int = 0
_race_losses: int = 0
_race_lock         = threading.Lock()

# Race-lost bookkeeping: track competitor deposit amounts we've seen so
# we can detect when their tx/included arrives and we lost the race to them.
_race_lost_amounts: dict = {}
_race_lost_lock            = threading.Lock()

_recent_alloc:      dict = {}
_recent_alloc_lock         = threading.Lock()

_pending_activations: dict = {}
_pending_lock               = threading.Lock()

# tx/included fast-path: fire stake_activate on tx/included instead of waiting
# for the deposit contract event. Toggleable via dashboard checkbox.



# Sweeper-owned activations — these come from rotation.py, not deposit race.
# When an activate event arrives for one of these addresses, we skip deposit log.
_sweeper_activations: set  = set()
_sweeper_lock               = threading.Lock()

# [deposit_seed_split]
SEED_DUSK: float            = 1000.0
_SEED_INFLIGHT_SECS: float  = 180.0

def mark_sweeper_activation(addr: str) -> None:
    """Called by rotation.py after sweeper fires stake_activate.
    Prevents the resulting activate contract event from polluting deposit race log."""
    with _sweeper_lock:
        _sweeper_activations.add(addr)


def _dlog(entry: dict) -> None:
    entry.setdefault("ts", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    with _deposit_log_lock:
        _deposit_log.appendleft(entry)
    _log(f"[events] {entry.get('msg', '')}")


def get_deposit_log() -> list:
    with _deposit_log_lock:
        return list(_deposit_log)


def get_race_counters() -> dict:
    # [deposit_race_pause] counter response
    with _race_lock:
        return {
            "wins":   _race_wins,
            "losses": _race_losses,
            "paused": bool(cfg("deposit_race_paused")),
        }


def _now_ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _own_addresses() -> set:
    # [six_nodes] every configured provisioner, not a fixed 0..3 — otherwise
    # txs of the static masters count as a competitor's.
    from .config import NODE_INDICES
    addrs = set()
    for i in NODE_INDICES:
        v = cfg(f"prov_{i}_address")
        if v:
            addrs.add(v)
    return addrs


def _current_window(block_height: int | None) -> str:
    rot_win    = int(cfg("rotation_window") or 41)
    snatch_win = int(cfg("snatch_window")   or 11)
    if not block_height:
        return "regular"
    blk_left = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)
    if blk_left <= snatch_win:
        return "snatch"
    if blk_left <= rot_win:
        return "rotation"
    return "regular"


def on_event(topic: str, decoded: dict, block_height: int | None = None) -> None:
    if not isinstance(decoded, dict):
        return
    try:
        if topic == "deposit":
            _handle_deposit(decoded, block_height, label="deposit")
        elif topic == "donate":
            # [airdrop] sozu_airdrop emits `donate` (confirmed with the contract
            # developers). DonateEvent is {account, amount} — same shape as
            # DepositEvent minus fee/token_total_supply, because an airdrop adds
            # DUSK to the pool without minting shares. Same effect on pool
            # balance as a deposit, so it races on exactly the same path.
            _handle_deposit(decoded, block_height, label="airdrop")
        elif topic == "reward":
            _handle_reward(decoded, block_height)
        elif topic in ("activate", "liquidate", "deactivate", "unstake"):
            # Invalidate assess + capacity caches — stake state just changed on-chain
            try:
                from .assess import _invalidate_all_caches
                _invalidate_all_caches()
            except Exception:
                pass
            if topic == "activate":
                _handle_activate(decoded, block_height)
        elif topic == "tx/executed":
            _handle_tx_executed(decoded, block_height)
    except Exception as e:
        _log(f"[events] on_event({topic}) error: {e}")


def _handle_reward(decoded: dict, block_height: int | None) -> None:
    operation   = decoded.get("operation", "unknown")
    amount_lux  = int(str(decoded.get("amount") or 0))
    window      = _current_window(block_height)

    if operation == "recycle":
        _handle_deposit(
            {"amount": amount_lux, "hash": "", "account": "reward"},
            block_height, label="reward/recycle",
        )
    elif operation == "terminate":
        if window == "snatch":
            _handle_deposit(
                {"amount": amount_lux, "hash": "", "account": "reward"},
                block_height, label="reward/terminate",
            )
        else:
            provisioners = decoded.get("provisioners") or []
            own = _own_addresses()
            own_terminated = [p for p in provisioners if p in own]
            if own_terminated:
                _handle_deposit(
                    {"amount": amount_lux, "hash": "", "account": "reward"},
                    block_height, label="reward/terminate(own)",
                )
            else:
                _log(f"[events] reward/terminate: no own prov in list — skip ({window})")
    else:
        _log(f"[events] reward: unknown operation={operation!r} — skip")


def _handle_deposit(decoded: dict, block_height: int | None, label: str = "deposit") -> None:
    amount_lux  = int(str(decoded.get("amount") or decoded.get("value") or 0))
    amount_dusk = amount_lux / 1e9
    depositor   = decoded.get("account") or decoded.get("sender") or ""

    dedup_key = f"{label}|{amount_dusk:.4f}"
    now = time.time()
    with _recent_alloc_lock:
        if now - _recent_alloc.get(dedup_key, 0) < 30:
            _log(f"[events] dedup skip {label} {amount_dusk:.4f}")
            return
        _recent_alloc[dedup_key] = now
        for k in list(_recent_alloc):
            if now - _recent_alloc[k] > 60:
                del _recent_alloc[k]

    # Skip if rotation is actively running — it owns pool allocation right now
    try:
        from .rotation import is_rotation_in_progress
        if is_rotation_in_progress():
            _log(f"[events] deposit race paused — rotation in progress")
            return
    except Exception:
        pass

    # [deposit_pre_rotation_pause] Optional (0 = off): hold the race from
    # `deposit_pause_pre_rotation_blocks` blocks before the rotation window
    # until this epoch's rotation has started. A top-up still in the mempool
    # when the rotation liquidates collides on the spendId (testnet 2153).
    # The deposit stays in the pool for the rotation / sweeper.
    _pre_blocks = int(cfg("deposit_pause_pre_rotation_blocks") or 0)
    if _pre_blocks > 0 and block_height:
        try:
            from .rotation import rotation_started_this_epoch
            _rot_win    = int(cfg("rotation_window") or 41)
            _snatch_win = int(cfg("snatch_window")   or 11)
            _blk_left   = EPOCH_BLOCKS - (block_height % EPOCH_BLOCKS)
            _cur_epoch  = block_height // EPOCH_BLOCKS + 1
            if (_snatch_win < _blk_left <= _rot_win + _pre_blocks
                    and not rotation_started_this_epoch(_cur_epoch)):
                _log(f"[events] deposit race held — {_blk_left} blocks left, rotation not started yet "
                     f"(deposit_pause_pre_rotation_blocks={_pre_blocks})")
                _dlog({"type":"deposit_skipped", "step":0,
                       "msg":f"⏸ {label} {amount_dusk:,.4f} DUSK held before rotation "
                             f"(blk_left={_blk_left}) — stays in pool",
                       "amount":amount_dusk, "window":_current_window(block_height), "ok":False})
                return
        except Exception as _pe:
            _log(f"[events] pre-rotation pause check failed: {_pe} — racing")

    # [deposit_race_pause] pause check
    # Manual pause toggle from dashboard. When set, deposits are logged
    # and skipped; W/L counters are NOT updated. Resume toggles back.
    if cfg("deposit_race_paused"):
        _amount_lux  = int(str(decoded.get("amount") or decoded.get("value") or 0))
        _amount_dusk = _amount_lux / 1e9
        _window      = _current_window(block_height)
        _dlog({"type":"deposit_skipped", "step":0,
               "msg":f"⏸ deposit race PAUSED — {label} {_amount_dusk:,.4f} DUSK ({_window}) stays in pool",
               "amount":_amount_dusk, "window":_window, "ok":False})
        return

    window         = _current_window(block_height)
    min_dep        = float(cfg("min_deposit_dusk")        or 100.0)
    snatch_min_dep = float(cfg("snatch_min_deposit_dusk") or 100.0)
    threshold      = snatch_min_dep if window == "snatch" else min_dep

    # TODO: re-enable: if amount_dusk < threshold:
    if False:
        _dlog({"type":"deposit_skipped","step":0,
               "msg":f"{label} {amount_dusk:,.4f} DUSK < {threshold:,.0f} min ({window}) — skip",
               "amount":amount_dusk,"window":window,"ok":False})
        return

    # [deposit_seed_split] Rotation pair one node short → spend 1000 of this
    # deposit on seeding the inactive rotation node, remainder tops up the
    # partner (rot_active, or rot_slave inside the window) in the same block.
    split = _pick_seed_split(amount_lux, window)
    if split:
        seed_idx, seed_addr, act_idx, act_addr, act_active = split
        _gk = f"{round(amount_lux/1e9,4)}|{round(time.time())}"
        _rest = (f" + rest → prov[{act_idx}] (batch)…" if act_idx is not None
                 else " (rest stays in pool)…")
        _dlog({
            "type":      "deposit_received",
            "step":      1,
            "msg":       (f"{label} {amount_dusk:,.4f} DUSK ({window}) → rot pair short one node: "
                          f"seed prov[{seed_idx}] {SEED_DUSK:,.0f}{_rest}"),
            "amount":    amount_dusk,
            "window":    window,
            "depositor": depositor,
            "block":     block_height,
            "prov_idx":  seed_idx,
            "prov_addr": seed_addr,
            "group_key": _gk,
            "ok":        True,
        })
        threading.Thread(
            target=_do_seed_split,
            args=(seed_idx, seed_addr, act_idx, act_addr, act_active, amount_lux, block_height, _gk),
            daemon=True,
        ).start()
        return

    target_idx, target_addr = _pick_target(window)

    if target_idx is None:
        _dlog({"type":"deposit_error","step":1,
               "msg":f"{label} {amount_dusk:,.4f} DUSK ({window}) → no eligible target (need ta=0, or ta=1 in rot/snatch window) — stake stays in pool",
               "amount":amount_dusk,"window":window,"ok":False})
        return

    # Generate group_key here so step 1 log and all _do_allocate log entries share the same group
    import time as _t
    _gk = f"{round(amount_lux/1e9,4)}|{round(_t.time())}"
    try:
        from .assess import _assess_state_cached
        _nodes_ta = (_assess_state_cached(0, "").get("by_idx", {})
                     .get(target_idx, {}).get("ta"))
    except Exception:
        _nodes_ta = "?"
    _dlog({
        "type":      "deposit_received",
        "step":      1,
        "msg":       f"{label} {amount_dusk:,.4f} DUSK ({window}) → targeting prov[{target_idx}] (ta={_nodes_ta})…",
        "amount":    amount_dusk,
        "window":    window,
        "depositor": depositor,
        "block":     block_height,
        "prov_idx":  target_idx,
        "prov_addr": target_addr,
        "group_key": _gk,
        "ok":        True,
    })

    threading.Thread(
        target=_do_allocate,
        args=(target_idx, target_addr, amount_lux, block_height, _gk),
        daemon=True,
    ).start()


def _pick_target(window: str = "regular") -> tuple:
    """
    Target for deposit race allocation, scoped to ROTATION_PAIR=(2,3) by
    architectural invariant (lib/config.py). Master pair is redistribute-managed and
    must never receive deposit-race allocations.

      Priority 1: rot_active (ta=0) — immediate effect, capacity+slash checked
      Priority 2: rot_slave  (ta=1) — active next epoch, no slash penalty
                  (only considered in rotation/snatch window)
      No target   → log and leave in pool (ta=2/inactive have no near-term effect)

    Uses cached assess + capacity — no wallet subprocess on hot path.
    """
    from .assess import _assess_state_cached, _fetch_capacity_cached, _max_topup_active
    try:
        pw = ""
        try:
            from .wallet import get_password
            pw = get_password() or ""
        except Exception:
            pass

        st    = _assess_state_cached(0, pw)   # cached, no subprocess
        nodes = st.get("by_idx", {})

        # Priority 1: active rotation node with headroom
        for idx in ROTATION_PAIR:
            node = nodes.get(idx, {})
            if node.get("status") == "active":
                try:
                    cap      = _fetch_capacity_cached(pw)  # cached, no subprocess
                    headroom = _max_topup_active(cap)
                    if headroom <= 0:
                        _dlog({
                            "type": "deposit_skipped",
                            "step": 0,
                            "msg":  f"rot_active prov[{idx}] slash headroom exhausted — stake stays in pool",
                            "ok":   False,
                        })
                        return None, None
                    addr = node.get("staking_address") or cfg(f"prov_{idx}_address") or ""
                    if addr:
                        return idx, addr
                except Exception as e:
                    _log(f"[events] capacity check error: {e}")

        # Priority 2: maturing ta=1 (rot_slave) — only in rotation/snatch window
        # In regular window ta=1 is skipped: stake would sit idle for up to 6h
        # and a competitor may have a better active target.
        if window in ("rotation", "snatch"):
            for idx in ROTATION_PAIR:
                node = nodes.get(idx, {})
                if node.get("status") in ("maturing", "seeded") and node.get("ta") == 1:
                    addr = node.get("staking_address") or cfg(f"prov_{idx}_address") or ""
                    if addr:
                        return idx, addr

        # Priority 3 intentionally removed — ta=2/inactive nodes are not valid deposit
        # race targets. Stake on these nodes has no immediate effect. Leave in pool;
        # sweeper or next rotation will handle it.

    except Exception as e:
        _log(f"[events] _pick_target error: {e}")
    return None, None


def _do_allocate(idx: int, addr: str, amount_lux: int, deposit_block: int | None, group_key: str = "") -> None:
    """Fire stake-activate. Amount in LUX (integer, 9 decimal places). Step 2 result from tx/executed."""
    from .wallet import operator_cmd, get_password, WALLET_PATH
    from .config import _NET

    pw = get_password() or ""

    # Cap to available capacity. Two dimensions matter:
    #   1. eligibility — active_maximum - active_current (how much active stake total)
    #   2. slash collateral — locked_maximum - locked_current (10% backing)
    # Top-up to an ACTIVE node consumes BOTH dimensions: contract requires
    #   locked_added = topup_lux // 10 of headroom, else panics with
    #   "operator should have enough locked capacity".
    # _max_topup_active() returns the max top-up that respects locked headroom.
    # For non-active targets (maturing/seeded/inactive), only eligibility applies.
    try:
        from .assess import _fetch_capacity_cached, _invalidate_capacity_cache, _max_topup_active, _assess_state_cached
        cap   = _fetch_capacity_cached(pw)
        nodes = _assess_state_cached(0, pw).get("by_idx", {})
        target_status = (nodes.get(idx) or {}).get("status", "")
        is_active     = target_status == "active"

        # Eligibility headroom (dimension 1) — applies to all targets
        elig_dusk = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
        elig_lux  = int(round(elig_dusk * 1e9))

        # Slash headroom (dimension 2) — applies only to active top-ups
        if is_active:
            slash_dusk = _max_topup_active(cap)
            slash_lux  = int(round(slash_dusk * 1e9))
            available_lux = min(elig_lux, slash_lux)
            cap_reason    = "active top-up: limited by min(eligibility, slash×10)"
        else:
            available_lux = elig_lux
            cap_reason    = f"{target_status} target: limited by eligibility only"

        min_dep_lux = round(float(cfg("min_deposit_dusk") or 100.0) * 1e9)
        if available_lux < min_dep_lux:
            _dlog({"type": "deposit_skipped", "step": 1,
                   "msg": f"capacity full ({cap_reason}) — available={available_lux/1e9:.4f} DUSK < {min_dep_lux/1e9:.0f} DUSK min — stake stays in pool",
                   "amount": amount_lux / 1e9, "group_key": group_key, "ok": False})
            with _pending_lock:
                _pending_activations.pop(addr, None)
            return
        if amount_lux > available_lux:
            _log(f"[events] capping deposit {amount_lux/1e9:.4f} → {available_lux/1e9:.4f} DUSK ({cap_reason}), remainder stays in pool")
            amount_lux = available_lux
        # [deposit_race_no_clamp] We intentionally skip [pool_balance_clamp] here.
        # The clamp's chain query adds 50-200ms latency, which loses the deposit
        # race against competitors. Protective value here is low: a deposit just
        # landed (pool capacity INCREASED) and amount_lux is already capped to
        # available_lux. Rare panic case = one wasted tx, same as losing the
        # race. Keep clamp for slow paths (rotation/sweeper).
        # Invalidate cache NOW before firing so concurrent threads re-fetch fresh capacity
        _invalidate_capacity_cache()
    except Exception as _cap_err:
        _log(f"[events] capacity cap error: {_cap_err}")

    amount_dusk = amount_lux / 1e9
    t_start     = time.time()

    amount_key = round(amount_dusk, 4)
    # Prefer group_key passed from _handle_deposit (matches step 1 log); fall back to local
    group_key  = group_key or f"{amount_key}|{round(t_start)}"

    with _pending_lock:
        _pending_activations[addr] = {
            "wall_ts":       t_start,
            "amount_dusk":   amount_dusk,
            "deposit_block": deposit_block,
            "prov_idx":      idx,
            "included_ts":   None,
            "group_key":     group_key,
        }

    # Register race candidate so competitor tx/included can be matched
    with _race_lost_lock:
        _race_lost_amounts[amount_key] = {
            "ts":                 t_start,
            "wall_ts":            t_start,
            "group_key":          group_key,
            "winner_included_ts": None,
        }

    result = operator_cmd(
        f"pool stake-activate --skip-confirmation "
        f"--amount {amount_lux} "
        f"--provisioner {addr} "
        f"--provisioner-wallet {WALLET_PATH} "
        f"--provisioner-password '{pw}'",
        timeout=120, password=pw,
    )
    # Store reaction time (start → tx sent) — rest is blockchain latency
    t_sent = time.time()
    with _pending_lock:
        p = _pending_activations.get(addr)
        if p:
            p["sent_ts"] = t_sent


def _pick_seed_split(amount_lux: int, window: str = "regular") -> tuple | None:
    """
    [deposit_seed_split] Rotation pair is one node short:
      A:1 M:0 I:1 — rot_active, no rot_slave (e.g. after a manual unstake)
      A:0 M:1 I:1 — rot_slave at ta=1, no rot_active (e.g. after a failed
                    [4/4] re-seed; testnet epoch 2076 — a 468k deposit went
                    entirely to the maturing node and prov[2] stayed empty)
    The state check would seed the inactive node from the pool, but the race
    empties the pool into the partner — or user unstakes drain it — before it
    ever holds 1000 DUSK. So a deposit ≥ SEED_DUSK is split instead: SEED_DUSK
    seeds the inactive node, the rest tops up the partner. An active partner
    takes the rest in any window; a maturing partner only in the rotation/snatch
    window (same rule as _pick_target) — otherwise the rest stays in the pool.

    Returns (seed_idx, seed_addr, partner_idx, partner_addr, partner_active) or
    None (→ normal race); partner_idx is None when only the seed leg is sent.
    Uses cached assess + capacity — no wallet subprocess on the hot path.
    """
    if amount_lux < round(SEED_DUSK * 1e9):
        return None
    try:
        from .rotation import is_rotation_enabled, is_seed_inflight
        if not is_rotation_enabled() or is_seed_inflight():
            return None

        from .assess import _assess_state_cached, _fetch_capacity_cached
        from .wallet import get_password
        pw    = get_password() or ""
        nodes = _assess_state_cached(0, pw).get("by_idx", {})
        active   = [i for i in ROTATION_PAIR if nodes.get(i, {}).get("status") == "active"]
        maturing = [i for i in ROTATION_PAIR if nodes.get(i, {}).get("status") in ("maturing", "seeded")]
        inactive = [i for i in ROTATION_PAIR if nodes.get(i, {}).get("status") == "inactive"]
        if len(inactive) != 1 or len(active) + len(maturing) != 1:
            return None
        seed_idx = inactive[0]
        if active:
            act_idx, act_active = active[0], True
        else:
            # Partner must be ta=1: seeding next to a ta=2 node leaves both at
            # ta=2 (bad stagger — the state check would deactivate one again).
            if nodes.get(maturing[0], {}).get("ta") != 1:
                return None
            act_idx, act_active = maturing[0], False
            if window not in ("rotation", "snatch"):
                act_idx = None      # seed only; rest stays in pool (see _pick_target)

        # Residual rewards need _seed_node's liquidate+terminate reset first —
        # too slow for a race; leave that case to the state check.
        if nodes.get(seed_idx, {}).get("reward_dusk", 0.0) > 0:
            return None

        # The CLI batch queue is shared with the dashboard's manual batch panel;
        # propagating would send the operator's queued txs along with ours.
        from . import batchq
        if batchq.entries():
            _log("[events] seed split skipped — manual batch queue not empty")
            return None

        cap = _fetch_capacity_cached(pw)
        if cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0) < SEED_DUSK:
            return None

        def _a(i: int) -> str:
            n = nodes.get(i, {})
            return cfg(f"prov_{i}_address") or n.get("staking_address") or n.get("address") or ""
        seed_addr = _a(seed_idx)
        act_addr  = _a(act_idx) if act_idx is not None else None
        if not seed_addr or (act_idx is not None and not act_addr):
            return None
        return seed_idx, seed_addr, act_idx, act_addr, act_active
    except Exception as e:
        _log(f"[events] _pick_seed_split error: {e}")
        return None


def _do_seed_split(seed_idx: int, seed_addr: str, act_idx: int | None, act_addr: str | None,
                   act_active: bool, amount_lux: int, deposit_block: int | None,
                   group_key: str) -> None:
    """
    [deposit_seed_split] Seed the inactive rot node with SEED_DUSK and top up
    the partner with the rest, both in ONE wallet batch → same block, nonces
    ordered by the wallet (two separate txs would collide in the mempool).
    Remainder too small / no headroom / no partner leg → seed alone, rest
    stays in pool. The slash clamp applies only to an active partner.
    """
    from .wallet import operator_cmd, get_password, tx_gate, WALLET_PATH
    from .rotation import claim_seed_inflight, release_seed_inflight
    from .rues import register_tx_confirm, get_tx_confirm_result

    if not claim_seed_inflight(_SEED_INFLIGHT_SECS):
        # A concurrent deposit got the seed — this one races normally.
        idx, addr = _pick_target(_current_window(deposit_block))
        if idx is not None:
            _do_allocate(idx, addr, amount_lux, deposit_block, group_key)
        return

    pw       = get_password() or ""
    seed_lux = round(SEED_DUSK * 1e9)
    rest_lux = (amount_lux - seed_lux) if act_idx is not None else 0
    try:
        from .assess import _fetch_capacity_cached, _invalidate_capacity_cache, _max_topup_active
        cap       = _fetch_capacity_cached(pw)
        elig_lux  = int(round(max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0)) * 1e9))
        rest_lux  = max(0, min(rest_lux, elig_lux - seed_lux))
        if act_active:
            rest_lux = min(rest_lux, int(round(_max_topup_active(cap) * 1e9)))
        _invalidate_capacity_cache()
    except Exception as e:
        _log(f"[events] seed split capacity error: {e} — seeding only")
        rest_lux = 0
    if rest_lux < round(float(cfg("min_deposit_dusk") or 100.0) * 1e9):
        rest_lux = 0

    legs = [(seed_idx, seed_addr, seed_lux)] + ([(act_idx, act_addr, rest_lux)] if rest_lux else [])
    t_start = time.time()
    for idx, addr, lux in legs:
        with _pending_lock:
            _pending_activations[addr] = {
                "wall_ts": t_start, "amount_dusk": lux / 1e9, "deposit_block": deposit_block,
                "prov_idx": idx, "included_ts": None, "group_key": group_key,
            }
        with _race_lost_lock:
            _race_lost_amounts[round(lux / 1e9, 4)] = {
                "ts": t_start, "wall_ts": t_start, "group_key": group_key,
                "winner_included_ts": None,
            }

    def _activate(addr: str, lux: int, batch: bool) -> dict:
        return operator_cmd(
            f"pool stake-activate --skip-confirmation{' --batch' if batch else ''} "
            f"--amount {lux} "
            f"--provisioner {addr} "
            f"--provisioner-wallet {WALLET_PATH} "
            f"--provisioner-password '{pw}'",
            timeout=120, password=pw,
        )

    seed_evt = register_tx_confirm("stake_activate", seed_addr)
    if len(legs) == 1:
        r = _activate(seed_addr, seed_lux, batch=False)
    else:
        with tx_gate:
            r = _activate(seed_addr, seed_lux, batch=True)
            if r.get("ok"):
                r = _activate(act_addr, rest_lux, batch=True)
            if r.get("ok"):
                r = operator_cmd("batch propagate --skip-confirmation --format json",
                                 timeout=180, password=pw)
            if not r.get("ok"):
                operator_cmd("batch clear", timeout=30, password=pw)

    split_str = f"seed prov[{seed_idx}] {SEED_DUSK:,.0f}" + (
        f" + prov[{act_idx}] {rest_lux/1e9:,.4f}" if rest_lux else " (rest stays in pool)")
    if not r.get("ok"):
        _dlog({"type": "deposit_error", "step": 1,
               "msg": f"seed split failed ({split_str}): {(r.get('stderr') or '')[:160]} — state check retries the seed",
               "amount": amount_lux / 1e9, "group_key": group_key, "ok": False})
        with _pending_lock:
            for _, addr, _ in legs:
                _pending_activations.pop(addr, None)
        release_seed_inflight()
        return

    with _pending_lock:
        for _, addr, _ in legs:
            if addr in _pending_activations:
                _pending_activations[addr]["sent_ts"] = time.time()
    _log(f"[events] seed split sent: {split_str} DUSK")

    # Hold the seed slot until the seed lands so the state check doesn't fire a
    # second seed for the same node; on timeout the claim just expires.
    if seed_evt.wait(timeout=120):
        res = get_tx_confirm_result("stake_activate", seed_addr)
        if (res or {}).get("err"):
            _log(f"[events] seed split: seed of prov[{seed_idx}] reverted: {str(res['err'])[:120]}")
        try:
            from .assess import _invalidate_assess_cache
            _invalidate_assess_cache()
        except Exception:
            pass
        release_seed_inflight()


def _warm_caches() -> None:
    """Pre-fetch assess + capacity so they're hot when reward event fires."""
    try:
        from .wallet import get_password
        from .assess import _assess_state_cached, _fetch_capacity_cached
        pw = get_password() or ""
        _assess_state_cached(0, pw, force=True)
        _fetch_capacity_cached(pw, force=True)
    except Exception as e:
        _log(f"[events] cache warm error: {e}")






def _handle_tx_executed(decoded: dict, block_height: int | None) -> None:
    """Step 2: tx/executed for stake_activate — win or loss."""
    global _race_wins, _race_losses
    # tx/executed always carries block_height in its payload; param may be None
    # depending on rues dispatcher. Prefer the payload value.
    block_height = decoded.get("block_height") or block_height
    inner = decoded.get("inner") or decoded
    call  = inner.get("call") or {}
    fn_name_dbg = call.get("fn_name")
    if fn_name_dbg != "stake_activate":
        return

    # Same extraction logic as rues.py — handle all 4 shapes:
    #   dict with 'provisioner' key, dict with 'keys.account' path,
    #   dict with 'provisioners' list, or bare string (liquidate/terminate).
    fn_dec = call.get("_fn_args_decoded") or {}
    if isinstance(fn_dec, str):
        prov_addr    = fn_dec
        prov_decoded = {}
    else:
        prov_addr    = (fn_dec.get("provisioner") or
                        (fn_dec.get("keys") or {}).get("account") or
                        ((fn_dec.get("provisioners") or [None])[0]) or "")
        prov_decoded = fn_dec

    amount_lux  = int(str(prov_decoded.get("value") or 0))
    amount_dusk = amount_lux / 1e9
    err         = decoded.get("err")
    gas_spent   = decoded.get("gas_spent")
    gas_limit   = (inner.get("fee") or {}).get("gas_limit")

    # Race-scope gate: tx must match an in-flight race we initiated.
    # _race_lost_amounts[amount_key] is set in _do_allocate before our tx fires
    # and survives ~60s. Any stake_activate tx in that window for the same amount
    # belongs to this race (ours or a competitor's).
    amount_key = round(amount_dusk, 4)
    with _race_lost_lock:
        race_entry = _race_lost_amounts.get(amount_key)
    if race_entry is None:
        return  # not a race we're tracking — manual UI op, sweeper, or stale event

    # Of the matched-amount txs, only OUR provisioner addresses count for the
    # win/loss counter. Competitor txs are handled by _handle_activate's
    # "lost_to" log (for their wins) and ignored entirely if they failed.
    own = _own_addresses()
    is_own = bool(prov_addr) and bool(own) and (prov_addr in own)
    if not is_own:
        return  # competitor's tx; not our counter

    # Optional timing enrichment from _pending_activations.
    with _pending_lock:
        pending = _pending_activations.get(prov_addr)

    now = time.time()
    if pending:
        exec_elapsed     = round(now - pending["wall_ts"], 1)
        included_elapsed = (round(pending["included_ts"] - pending["wall_ts"], 1)
                            if pending.get("included_ts") else None)
        pending_group_key = pending.get("group_key")
    else:
        exec_elapsed = None
        included_elapsed = None
        pending_group_key = race_entry.get("group_key") if race_entry else None

    def _timing_str() -> str:
        parts = []
        if included_elapsed is not None:
            parts.append(f"react {included_elapsed}s")
        if exec_elapsed is not None:
            parts.append(f"exec {exec_elapsed}s")
        return " · ".join(parts)

    if not err:
        with _race_lock:
            _race_wins += 1
        wins, losses = _race_wins, _race_losses
        win_group_key = pending_group_key or (race_entry.get("group_key") if race_entry else None)
        _dlog({
            "type":             "tx_confirmed",
            "step":             2,
            "msg":              (f"✓ tx confirmed blk #{block_height}"
                                 + (f" · {_timing_str()}" if _timing_str() else "")
                                 + f" · gas {gas_spent}/{gas_limit}"
                                 + f" · W{wins}/L{losses}"),
            "prov_addr":        prov_addr,
            "amount":           amount_dusk,
            "block":            block_height,
            "elapsed_s":        exec_elapsed,
            "included_elapsed": included_elapsed,
            "wins":             wins,
            "losses":           losses,
            "group_key":        win_group_key,
            "ok":               True,
        })
    else:
        with _race_lock:
            _race_losses += 1
        wins, losses = _race_wins, _race_losses
        _amt_key = round(amount_dusk, 4)
        with _race_lost_lock:
            _fb_key = (_race_lost_amounts.get(_amt_key) or {}).get("group_key")
        group_key = pending_group_key or _fb_key or f"{_amt_key}|loss"
        _dlog({
            "type":             "tx_failed",
            "step":             2,
            "msg":              (f"✗ tx failed blk #{block_height}: {err}"
                                 + (f" · {_timing_str()}" if _timing_str() else "")
                                 + f" · W{wins}/L{losses}"),
            "prov_addr":        prov_addr,
            "amount":           amount_dusk,
            "block":            block_height,
            "elapsed_s":        exec_elapsed,
            "included_elapsed": included_elapsed,
            "error":            str(err),
            "wins":             wins,
            "losses":           losses,
            "group_key":        group_key,
            "ok":               False,
        })






def _handle_activate(decoded: dict, block_height: int | None) -> None:
    """Step 3: activate contract event — stake is now live on-chain."""
    prov_addr   = decoded.get("provisioner") or decoded.get("account") or ""
    amount_lux  = int(str(decoded.get("amount") or decoded.get("value") or 0))
    amount_dusk = amount_lux / 1e9
    amount_key  = round(amount_dusk, 4)

    own = _own_addresses()

    # Look up race entry for this amount — but DO NOT pop it.
    # _handle_tx_executed needs it to gate the win/loss counter, and it may
    # arrive after this activate event (step 3 can fire before step 2 in some
    # RUES dispatch orderings). Entry expires by TTL only (60s).
    with _race_lost_lock:
        lost_entry = _race_lost_amounts.get(amount_key)
        # Clean stale entries (> 60s old)
        now = time.time()
        for k in list(_race_lost_amounts):
            if now - _race_lost_amounts[k]["ts"] > 60:
                del _race_lost_amounts[k]

    if lost_entry and prov_addr and (not own or prov_addr not in own):
        short        = prov_addr[:20] + "…" if len(prov_addr) > 20 else prov_addr
        wall_ts      = lost_entry.get("wall_ts")
        now          = time.time()
        winner_react = round(lost_entry["winner_included_ts"] - wall_ts, 1) \
                       if lost_entry.get("winner_included_ts") and wall_ts else None
        winner_exec  = round(now - wall_ts, 1) if wall_ts else None
        parts = []
        if winner_react is not None: parts.append(f"react {winner_react}s")
        if winner_exec  is not None: parts.append(f"exec {winner_exec}s")
        timing_str = (" · " + " · ".join(parts)) if parts else ""
        _dlog({
            "type":            "lost_to",
            "step":            2.5,
            "msg":             f"  lost to: {short} ({amount_dusk:,.4f} DUSK){timing_str}",
            "prov_addr":       prov_addr,
            "amount":          amount_dusk,
            "block":           block_height,
            "group_key":       lost_entry.get("group_key"),
            "winner_react_s":  winner_react,
            "winner_exec_s":   winner_exec,
            "ok":              False,
        })
        return

    # Own provisioner activate — only log if deposit race was in flight
    if prov_addr and own and prov_addr not in own:
        return

    # Step 3 own-activation log dropped: step 2 (tx_confirmed) already provides
    # the authoritative win signal with block height and timing. Just clean up
    # the pending entry.
    with _pending_lock:
        _pending_activations.pop(prov_addr, None)
