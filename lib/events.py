"""
events.py — RUES event action engine.

Allocation rules per window:
  Regular / Rotation:
    deposit           → allocate
    reward/recycle    → allocate
    reward/terminate  → allocate ONLY if own provisioner was terminated

  Snatch:
    deposit           → allocate
    reward/recycle    → allocate
    reward/terminate  → always allocate

3-step deposit race flow:
  Step 1 (deposit_received):  event arrived, stake-activate fired
  Step 2 (tx_confirmed/failed): tx/executed result — win/loss decided here
  Step 3 (activate_confirmed): activate contract event — stake is live on-chain

⚠ TODO:
  - Target selection incomplete: active top-up, multi-node split, capacity check,
    seed reservation, configurable override
  - Threshold check bypassed (if False) — re-enable when stable
  - Deposit ownership filtering (depositor address) — "we need this later"
  - block_height None for contract events — wire up block cache backfill
"""

import threading
import time
from collections import deque
from datetime import datetime

from .config import _log, cfg

EPOCH_BLOCKS: int = 2160

_deposit_log:      deque = deque(maxlen=200)
_deposit_log_lock         = threading.Lock()

_race_wins:   int = 0
_race_losses: int = 0
_race_lock         = threading.Lock()

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
    with _race_lock:
        return {"wins": _race_wins, "losses": _race_losses}


def _now_ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _own_addresses() -> set:
    addrs = set()
    for k in ("prov_0_address", "prov_1_address", "prov_2_address", "prov_3_address"):
        v = cfg(k)
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

    _master    = cfg("master_idx")
    master_idx = int(_master) if _master is not None else -1
    target_idx, target_addr = _pick_target(master_idx, window)

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


def _pick_target(master_idx: int, window: str = "regular") -> tuple:
    """
    Target for deposit race allocation:
      Priority 1: rot_active (ta=0, non-master) — immediate effect, capacity+slash checked
      Priority 2: rot_slave (ta=1, non-master)  — active next epoch, no slash penalty
      No target   → log and leave in pool (ta=2/inactive have no near-term effect)

    Uses cached assess + capacity — no wallet subprocess on hot path.
    """
    from .assess import _assess_state_cached, _fetch_capacity_cached, _max_topup_active
    from .config import NODE_INDICES
    try:
        pw = ""
        try:
            from .wallet import get_password
            pw = get_password() or ""
        except Exception:
            pass

        st    = _assess_state_cached(0, pw)   # cached, no subprocess
        nodes = st.get("by_idx", {})

        # Priority 1: active non-master with headroom
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
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
            for idx in NODE_INDICES:
                if idx == master_idx:
                    continue
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

    # Cap to available global capacity — all in integer LUX, no float rounding
    try:
        from .assess import _fetch_capacity_cached, _invalidate_capacity_cache
        cap             = _fetch_capacity_cached(pw)
        available_dusk  = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
        available_lux   = int(round(available_dusk * 1e9))
        min_dep_lux     = round(float(cfg("min_deposit_dusk") or 100.0) * 1e9)
        if available_lux < min_dep_lux:
            _dlog({"type": "deposit_skipped", "step": 1,
                   "msg": f"capacity full — available={available_lux/1e9:.4f} DUSK < {min_dep_lux/1e9:.0f} DUSK min — stake stays in pool",
                   "amount": amount_lux / 1e9, "group_key": group_key, "ok": False})
            with _pending_lock:
                _pending_activations.pop(addr, None)
            return
        if amount_lux > available_lux:
            _log(f"[events] capping deposit {amount_lux/1e9:.4f} → {available_lux/1e9:.4f} DUSK (max capacity), remainder stays in pool")
            amount_lux = available_lux
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
        f"{_NET} pool stake-activate --skip-confirmation "
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
    inner = decoded.get("inner") or decoded
    call  = inner.get("call") or {}
    if call.get("fn_name") != "stake_activate":
        return

    prov_decoded = call.get("_fn_args_decoded") or {}
    prov_addr    = (prov_decoded.get("keys") or {}).get("account") or ""
    amount_lux   = int(str(prov_decoded.get("value") or 0))
    amount_dusk  = amount_lux / 1e9
    err          = decoded.get("err")
    gas_spent    = decoded.get("gas_spent")
    gas_limit    = (inner.get("fee") or {}).get("gas_limit")

    own = _own_addresses()
    if prov_addr and own and prov_addr not in own:
        return

    with _pending_lock:
        pending = _pending_activations.get(prov_addr)

    now = time.time()
    exec_elapsed     = round(now - pending["wall_ts"], 1)      if pending else None
    included_elapsed = round(pending["included_ts"] - pending["wall_ts"], 1) \
                       if pending and pending.get("included_ts") else None
    pending_group_key = (pending or {}).get("group_key")

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

    # Check if this activate matches an amount we just lost — log who won
    with _race_lost_lock:
        lost_entry = _race_lost_amounts.pop(amount_key, None)
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

    with _pending_lock:
        pending = _pending_activations.pop(prov_addr, None)

    if not pending:
        return

    elapsed = round(time.time() - pending["wall_ts"], 1)
    _dlog({
        "type":      "activate_confirmed",
        "step":      3,
        "msg":       (f"✓ stake live on-chain blk #{block_height}"
                      + (f" · {elapsed}s total" if elapsed else "")),
        "prov_addr": prov_addr,
        "amount":    amount_dusk,
        "block":     block_height,
        "elapsed_s": elapsed,
        "ok":        True,
    })
