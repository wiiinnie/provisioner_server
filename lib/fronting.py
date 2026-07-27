"""
fronting.py — substrate unstake-target (fronting) check.

Unstakes/withdrawals hit ONE operator per epoch: the operator with the
HIGHEST max_eligibility among operators with active_current > 0. Redistribute
uses this to defer pre-seeding the fronting node while we are the target
(the 1k seed would be eaten by incoming unstakes).

Extracted from the removed heal.py — this was the only heal symbol with a
live consumer (redistribute.wants_preseed).
"""

import json
import threading

from .config import _log


# ── Unstake-target check (cache scoped to current epoch) ──────────────────────

_unstake_target_cache: dict = {"epoch": None, "is_us": None, "reason": ""}
_unstake_cache_lock = threading.Lock()


def _fetch_operators(pw: str) -> list:
    """Return list of registered operator BLS addresses from substrate."""
    from .rotation import _cmd
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
    return cached result until epoch changes. Fail-open (returns False on
    error) — callers treat "not the target" as the permissive default.
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
            _log("[fronting] unstake-target check: OPERATOR_ADDRESS not configured — assuming NOT us")
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
                _log(f"[fronting] unstake-target: capacity fetch failed for "
                     f"{op[:12]}…: {cap.get('error','?')[:60]}")
                continue
            if cap["active_current"] > 0:
                eligible.append((op, cap["active_maximum"]))
        if not eligible:
            _log("[fronting] unstake-target check: no operators with active stake — NOT us")
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
        _log(f"[fronting] unstake-target check (epoch {cur_epoch}): "
             f"{'US' if is_us else 'not us'} — {reason}")
        return is_us

    except Exception as e:
        _log(f"[fronting] unstake-target check failed: {e} — proceeding as NOT us (fail-open)")
        with _unstake_cache_lock:
            _unstake_target_cache = {"epoch": cur_epoch, "is_us": False,
                                      "reason": f"error: {e}"}
        return False
