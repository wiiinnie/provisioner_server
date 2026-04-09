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
_tx_included_fastpath: bool = False
_tx_included_fastpath_lock  = threading.Lock()


def set_tx_included_fastpath(enabled: bool) -> None:
    global _tx_included_fastpath
    with _tx_included_fastpath_lock:
        _tx_included_fastpath = enabled
    _log(f"[events] tx/included fast-path {'enabled' if enabled else 'disabled'}")


def get_tx_included_fastpath() -> bool:
    with _tx_included_fastpath_lock:
        return _tx_included_fastpath

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
    for k in ("prov_0_address", "prov_1_address", "prov_2_address"):
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
        elif topic == "activate":
            # Invalidate assess + capacity caches — stake state just changed
            try:
                from .assess import _invalidate_all_caches
                _invalidate_all_caches()
            except Exception:
                pass
            _handle_activate(decoded, block_height)
        elif topic == "tx/included" and get_tx_included_fastpath():
            _handle_tx_included_fastpath(decoded, block_height)
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
    target_idx, target_addr = _pick_target(master_idx)

    if target_idx is None:
        _dlog({"type":"deposit_error","step":1,
               "msg":f"no eligible provisioner for {label} — all active or master-only",
               "amount":amount_dusk,"window":window,"ok":False})
        return

    # Step 1
    _dlog({
        "type":      "deposit_received",
        "step":      1,
        "msg":       f"{label} {amount_dusk:,.4f} DUSK ({window}) → activating prov[{target_idx}]…",
        "amount":    amount_dusk,
        "window":    window,
        "depositor": depositor,
        "block":     block_height,
        "prov_idx":  target_idx,
        "prov_addr": target_addr,
        "ok":        True,
    })

    threading.Thread(
        target=_do_allocate,
        args=(target_idx, target_addr, amount_dusk, block_height),
        daemon=True,
    ).start()


def _pick_target(master_idx: int) -> tuple:
    """
    Target for deposit race allocation:
      Priority 1: rot_active (ta=0, non-master) with slash headroom available
      Priority 2: rot_slave (maturing ta=1, non-master) — no slash penalty
      Priority 3: any inactive/seeded non-master

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
                            "msg":  f"rot_active prov[{idx}] slash headroom exhausted — skipping",
                            "ok":   False,
                        })
                        return None, None
                    addr = node.get("staking_address") or cfg(f"prov_{idx}_address") or ""
                    if addr:
                        return idx, addr
                except Exception as e:
                    _log(f"[events] capacity check error: {e}")

        # Priority 2: maturing ta=1 (rot_slave)
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
            node = nodes.get(idx, {})
            if node.get("status") in ("maturing", "seeded") and node.get("ta") == 1:
                addr = node.get("staking_address") or cfg(f"prov_{idx}_address") or ""
                if addr:
                    return idx, addr

        # Priority 3: any inactive/seeded non-master
        for idx in NODE_INDICES:
            if idx == master_idx:
                continue
            if nodes.get(idx, {}).get("status") in ("inactive", "seeded", "maturing"):
                addr = nodes.get(idx, {}).get("staking_address") or cfg(f"prov_{idx}_address") or ""
                if addr:
                    return idx, addr

    except Exception as e:
        _log(f"[events] _pick_target error: {e}")
    return None, None


def _do_allocate(idx: int, addr: str, amount_dusk: float, deposit_block: int | None) -> None:
    """Fire stake-activate. Step 2 result comes from tx/executed event."""
    from .wallet import operator_cmd, get_password, WALLET_PATH
    from .config import _NET

    pw         = get_password() or ""
    amount_lux = int(amount_dusk * 1_000_000_000)
    t_start    = time.time()

    with _pending_lock:
        _pending_activations[addr] = {
            "wall_ts":       t_start,
            "amount_dusk":   amount_dusk,
            "deposit_block": deposit_block,
            "prov_idx":      idx,
            "included_ts":   None,   # set when tx/included arrives
        }

    # Register race candidate immediately so competitor tx/included can be matched
    # (competitor's tx/included arrives ~20s before we know we lost via tx/executed)
    amount_key = round(amount_dusk, 4)
    group_key  = f"{amount_key}|{round(t_start)}"
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


def _on_tx_included_own(prov_addr: str) -> None:
    """Called when tx/included arrives for our own stake_activate tx."""
    with _pending_lock:
        p = _pending_activations.get(prov_addr)
        if p and p.get("included_ts") is None:
            p["included_ts"] = time.time()


def _on_tx_included_competitor(prov_addr: str, amount_dusk: float) -> None:
    """Called when tx/included arrives for any stake_activate — record if it matches a pending loss."""
    own = _own_addresses()
    if own and prov_addr in own:
        return  # that's ours, handled by _on_tx_included_own
    amount_key = round(amount_dusk, 4)
    with _race_lost_lock:
        entry = _race_lost_amounts.get(amount_key)
        if entry and entry.get("winner_included_ts") is None:
            entry["winner_included_ts"] = time.time()


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
        # Retrieve the group_key registered at step 1
        amount_key = round(amount_dusk, 4)
        with _race_lost_lock:
            group_key = (_race_lost_amounts.get(amount_key) or {}).get("group_key", f"{amount_key}|loss")
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


def _handle_tx_included_fastpath(decoded: dict, block_height: int | None) -> None:
    """
    Fast-path: fire deposit race on tx/included, gaining ~1 block (~10s) over
    waiting for the contract event.

    deposit  — amount available in payload → fire immediately
    recycle  — amount not in payload → use pool cache as estimate
    terminate — amount not in payload → use pool cache as estimate

    Cache warming always runs regardless, so contract-event path is also faster.
    """
    try:
        contract = cfg("contract_id") or ""
        inner    = decoded.get("inner") or decoded
        call     = inner.get("call") or {}
        if call.get("contract") != contract:
            return
        fn_name = call.get("fn_name", "")

        # Always warm caches on any relevant tx/included — benefits both fast-path
        # and contract-event path if toggle is off
        if fn_name in ("deposit", "stake", "recycle", "terminate"):
            threading.Thread(target=_warm_caches, daemon=True).start()

        # Race firing only when toggle is enabled
        if not get_tx_included_fastpath():
            return

        if fn_name in ("deposit", "stake"):
            # Amount is in the payload
            fn_decoded = call.get("_fn_args_decoded") or {}
            amount_lux = int(str(fn_decoded.get("amount") or 0))
            if not amount_lux:
                return
            _handle_deposit(
                {"amount": amount_lux, "hash": decoded.get("hash", ""), "account": "tx/included"},
                block_height, label="deposit/tx_included",
            )

        elif fn_name in ("recycle", "terminate"):
            # fn_args is empty — amount not in tx/included payload.
            # Use pool cache as estimate; exact amount arrives via reward contract event.
            label = f"reward/{fn_name}/tx_included"
            threading.Thread(
                target=_fastpath_pool_estimate,
                args=(label, block_height),
                daemon=True,
            ).start()

    except Exception as e:
        _log(f"[events] tx/included fast-path error: {e}")


def _fastpath_pool_estimate(fn_name: str, block_height: int | None) -> None:
    """Fire deposit race using pool cache for recycle/terminate fast-path."""
    try:
        from .pool import _fast_alloc_pool
        pool_dusk = _fast_alloc_pool()
        min_dep   = float(cfg("min_deposit_dusk") or 100.0)
        if pool_dusk < min_dep:
            return
        amount_lux = int(pool_dusk * 1e9)
        _handle_deposit(
            {"amount": amount_lux, "hash": "", "account": "tx/included"},
            block_height, label=f"{fn_name}/tx_included",
        )
    except Exception as e:
        _log(f"[events] fastpath pool estimate error: {e}")


# ── Lost-to tracking ──────────────────────────────────────────────────────────
# When we lose a race, store the amount so we can match the winning activate event.
_race_lost_amounts: dict = {}   # amount_dusk (rounded) → {"ts": float, "group_key": str}
_race_lost_lock          = threading.Lock()


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
