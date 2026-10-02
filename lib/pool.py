"""
pool.py — SOZU pool balance cache and contract balance query.

Imports: config, wallet.
"""
import json
import subprocess
import threading
import time

from .config import (
    _log, CONTRACT_ID, CONTRACT_ADDRESS, RUSK_VERSION, _NODE_STATE_URL,
)

# ── Pool balance cache ─────────────────────────────────────────────────────────
_pool_balance_cache:    float = 0.0
_pool_balance_cache_ts: float = 0.0
_pool_balance_lock            = threading.Lock()
POOL_CACHE_STALE_SECS: int    = 300


def _query_contract_total_dusk(strict: bool = False) -> float:
    """Query total allocatable DUSK in the SOZU pool contract.

    strict=True raises on a failed/undecodable query instead of returning 0.0,
    for callers that must tell "pool is empty" from "query failed".
    """
    url = (f"{_NODE_STATE_URL}/on/contracts:"
           "0100000000000000000000000000000000000000000000000000000000000000"
           "/contract_balance")
    rc = subprocess.run(
        ["curl", "-s", "-X", "POST", url,
         "-H", f"rusk-version: {RUSK_VERSION}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(CONTRACT_ID)],
        capture_output=True, text=True, timeout=15)
    raw = rc.stdout.strip()
    try:
        return int(json.loads(raw)) / 1_000_000_000
    except Exception:
        pass
    try:
        import re, struct as _struct
        hex_match = re.search(r'[0-9a-fA-F]{16}', raw)
        if hex_match:
            return _struct.unpack("<Q", bytes.fromhex(hex_match.group(0)))[0] / 1_000_000_000
    except Exception:
        pass
    _log(f"[pool] contract balance decode failed: {raw!r}")
    if strict:
        raise ValueError(f"pool balance decode failed: {raw[:80]!r}")
    return 0.0


def _query_pool_balance_dusk(staked_dusk: float = 0.0, strict: bool = False) -> float:
    """Query allocatable DUSK in the SOZU pool. stdout only — not rotation log."""
    available = _query_contract_total_dusk(strict=strict)
    _log(f"[pool] balance: {available:.4f} DUSK")
    return available


def _pool_fetch_real(pw: str = "") -> float:
    """Fetch authoritative pool balance, update cache, return DUSK."""
    global _pool_balance_cache, _pool_balance_cache_ts
    dusk = _query_pool_balance_dusk()
    with _pool_balance_lock:
        _pool_balance_cache    = dusk
        _pool_balance_cache_ts = time.time()
    return dusk


def _pool_delta(delta_dusk: float) -> None:
    """Apply signed delta to pool balance cache (event-driven update)."""
    global _pool_balance_cache
    with _pool_balance_lock:
        _pool_balance_cache = max(0.0, _pool_balance_cache + delta_dusk)


def _fast_alloc_pool() -> float:
    """Return pool balance from cache — no network call."""
    with _pool_balance_lock:
        return _pool_balance_cache


# ── [pool_balance_clamp] safety helper ────────────────────────────────────
def clamp_to_pool_balance(intended_dusk: float, ctx: str, reserve_dusk: float = 0.0) -> float:
    """Cap intended stake_activate amount at the actual on-chain pool balance.

    [seed_pool_reserve] `reserve_dusk` is DUSK that must still be in the pool
    AFTER this activation — e.g. the 1000 DUSK re-seed that follows the [3/4]
    rotation allocation. The clamp is applied to (pool − reserve), so a user
    unstake served from the pool between our liquidate and this activation
    shrinks the allocation, not the seed. (Testnet epoch 2076, 2026-10-02: a
    26.8k unstake landed in the terminate block, the clamp handed the whole
    pool to rot_slave, and the re-seed reverted "should not exceed the
    pending stake".)

    The chain rejects stake_activate when amount > pool_contract_balance with:
        'Panic: [pool] The stake activation amount should not exceed the
         pending stake'

    Local accounting (our liquidations + terminations + rewards) can drift
    below chain reality between when we compute `intended` and when we fire
    the activation — from deposit-race recycles, user unstakes, other
    operators consuming the shared pool, or timing races.

    Query the chain fresh (no cache) right before activating. Use the lesser
    of intended vs actual. If query fails, return intended unchanged (don't
    deadlock the rotation cycle on a network blip — chain panic is a
    smaller risk than freezing).

    Returns DUSK amount to actually activate. Caller should still apply its
    own minimum-threshold check (e.g. < 10 DUSK → skip).
    """
    try:
        # strict: a failed query must not read as "pool empty" — that would
        # clamp a full rotation allocation to 0 and strand it through snatch.
        pool_dusk = _query_pool_balance_dusk(strict=True)
    except Exception as e:
        _log(f"[pool-clamp] {ctx}: balance query failed ({e}) — "
             f"proceeding with intended {intended_dusk:.4f} DUSK")
        return intended_dusk

    available = max(0.0, pool_dusk - max(0.0, reserve_dusk))
    if intended_dusk > available:
        _res = f" − reserve {reserve_dusk:.0f}" if reserve_dusk > 0 else ""
        _log(f"[pool-clamp] {ctx}: drift — intended {intended_dusk:.4f} > "
             f"pool {pool_dusk:.4f}{_res} DUSK — clamping to {available:.4f}")
        return available
    return intended_dusk
