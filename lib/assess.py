"""
assess.py — Stake-info fetching, capacity fetching, and per-provisioner state classification.

Imports: config, wallet.
"""
import json
import re
import threading
import time

from .config import (
    _log, NETWORK_ID, OPERATOR_ADDRESS, NODE_INDICES, RUSK_VERSION, GRAPHQL_URL,
    cfg, _NET, CONTRACT_ID,
)
from .wallet import operator_cmd, wallet_cmd

EPOCH_BLOCKS    = 2160
TOPUP_SLASH_RATE = 0.10

# ── Stake cache ────────────────────────────────────────────────────────────────
_stake_cache:      dict = {}
_stake_cache_lock        = threading.Lock()

# ── Stake history snapshots ───────────────────────────────────────────────────
import collections as _collections
_HISTORY_MAXLEN          = 6720          # 7d at 90s/snapshot cadence (Dusk ~10s/block)
_stake_history: _collections.deque = _collections.deque(maxlen=_HISTORY_MAXLEN)
_stake_history_lock      = threading.Lock()
# Events are read directly from rues._event_log at query time — no separate store needed


def _record_snapshot(block: int, nodes_by_idx: dict) -> None:
    """Store a stake snapshot keyed by block height."""
    if not block:
        return
    entry = {
        "block": block,
        "nodes": {
            idx: {
                "stake_dusk":  n.get("stake_dusk", 0.0),
                "locked_dusk": n.get("locked_dusk", 0.0),
                "reward_dusk": n.get("reward_dusk", 0.0),
                "status":      n.get("status", "inactive"),
            }
            for idx, n in nodes_by_idx.items()
        }
    }
    with _stake_history_lock:
        _stake_history.append(entry)



def get_stake_history(max_blocks: int = 2160) -> dict:
    """Return snapshots and events within the last max_blocks blocks.
    Events are pulled directly from rues._event_log and resolved to
    provisioner indices via _stake_cache (no config address dependency).
    """
    with _stake_history_lock:
        snaps = list(_stake_history)

    if not snaps:
        return {"snapshots": [], "events": []}

    cutoff = snaps[-1]["block"] - max_blocks
    snaps  = [s for s in snaps if s["block"] >= cutoff]

    # Build address → idx map from stake cache.
    # Build address → idx: config first (covers inactive nodes not in cache),
    # then stake cache (covers live staking addresses).
    addr_to_idx = {}
    try:
        from .config import NODE_INDICES, cfg as _cfg
        for i in NODE_INDICES:
            a = _cfg(f"prov_{i}_address") or ""
            if a:
                addr_to_idx[a] = i
    except Exception:
        pass
    with _stake_cache_lock:
        for idx, node in _stake_cache.items():
            for field in ("address", "staking_address"):
                addr = node.get(field) or ""
                if addr:
                    addr_to_idx[addr] = idx

    # Event extraction strategy:
    # Primary source: contract events (always have provisioner + amount).
    # Enrichment: match tx/executed by (topic, amount_lux) to add block_height.
    # This works even when _fn_args_decoded is missing from tx/executed entries.
    evts = []
    try:
        from .rues import _event_log, _log_lock
        _TOPIC_TO_FNNAME = {
            "activate":   "stake_activate",
            "liquidate":  "liquidate",
            "terminate":  "terminate",
            "deactivate": "stake_deactivate",
            "unstake":    "sozu_unstake",
        }
        _CONTRACT_TOPICS = frozenset(_TOPIC_TO_FNNAME.keys())
        _FNNAME_MAP = {v: k for k, v in _TOPIC_TO_FNNAME.items()}

        with _log_lock:
            raw_events = list(_event_log)

        # Pass 1: build block_height lookup from tx/executed events.
        # _fn_args_decoded shape differs by fn_name:
        #   stake_activate:       dict with 'value' (amount) + 'keys.account' (BLS addr)
        #   stake_deactivate /
        #   liquidate / terminate: string (just the BLS addr)
        # So we maintain two indexes and try addr first, amount as fallback.
        tx_block_by_addr   = {}   # (display_topic, bls_addr)       → block
        tx_block_by_amount = {}   # (display_topic, amount_lux_str) → block
        for entry in raw_events:
            if entry.get("topic") != "tx/executed":
                continue
            decoded = entry.get("decoded") or {}
            if decoded.get("err") is not None:
                continue
            inner   = decoded.get("inner") or decoded
            call    = inner.get("call") or {}
            fn_name = call.get("fn_name", "")
            display_topic = _FNNAME_MAP.get(fn_name)
            if not display_topic:
                continue
            block = int(decoded.get("block_height") or 0)
            if not block:
                continue
            fn_dec = call.get("_fn_args_decoded") or {}
            if isinstance(fn_dec, str):
                # deactivate / liquidate / terminate: args are just the BLS address
                tx_block_by_addr[(display_topic, fn_dec)] = block
            elif isinstance(fn_dec, dict):
                # stake_activate: has both addr + amount, index by both
                prov_key = (fn_dec.get("keys") or {}).get("account") \
                           or fn_dec.get("provisioner") or fn_dec.get("account")
                if prov_key:
                    tx_block_by_addr[(display_topic, prov_key)] = block
                amt_str = str(fn_dec.get("value") or fn_dec.get("amount_lux") or "")
                if amt_str:
                    tx_block_by_amount[(display_topic, amt_str)] = block

        # Pass 2: contract events — canonical source for provisioner + amount
        for entry in raw_events:
            topic = entry.get("topic", "")
            if topic not in _CONTRACT_TOPICS:
                continue
            decoded   = entry.get("decoded") or {}
            # provisioner/provisioners field first — "account" is the tx sender,
            # not the provisioner (e.g. unstake event: account=staker, provisioners=[our_prov])
            prov_addr = decoded.get("provisioner") or ""
            if not prov_addr:
                provs = decoded.get("provisioners") or []
                if provs:
                    prov_addr = provs[0]
            if not prov_addr:
                prov_addr = decoded.get("account") or ""

            amt = decoded.get("amount") or decoded.get("value")
            amt_str = str(amt) if amt is not None else ""
            try:
                amt_dusk = int(amt_str) / 1e9 if amt_str else None
            except Exception:
                amt_dusk = None

            # Enrich with block_height from tx/executed — addr first
            # (covers deactivate/liquidate/terminate), amount as fallback (activate).
            block = 0
            if prov_addr:
                block = tx_block_by_addr.get((topic, prov_addr), 0)
            if not block and amt_str:
                block = tx_block_by_amount.get((topic, amt_str), 0)

            idx = addr_to_idx.get(prov_addr)
            addr_short = (prov_addr[:10] + "…" + prov_addr[-6:]) if prov_addr else ""
            evts.append({"block": block, "idx": idx, "topic": topic,
                         "amount_dusk": amt_dusk, "addr_short": addr_short})

    except Exception:
        pass

    evts.sort(key=lambda e: e["block"])
    return {"snapshots": snaps, "events": evts}


# ── _assess_state cache ────────────────────────────────────────────────────────
_ASSESS_CACHE_SECS: int      = 90
_assess_cache_result: dict   = {}
_assess_cache_ts: float      = 0.0
_assess_cache_lock           = threading.Lock()

# ── Capacity cache ─────────────────────────────────────────────────────────────
_CAPACITY_CACHE_SECS: int    = 60
_capacity_cache_result: dict = {}
_capacity_cache_ts: float    = 0.0
_capacity_cache_lock         = threading.Lock()


def _invalidate_capacity_cache() -> None:
    global _capacity_cache_ts
    with _capacity_cache_lock:
        _capacity_cache_ts = 0.0


def _fetch_capacity_cached(pw: str, force: bool = False) -> dict:
    global _capacity_cache_result, _capacity_cache_ts
    with _capacity_cache_lock:
        age = time.time() - _capacity_cache_ts
        if not force and _capacity_cache_result and age < _CAPACITY_CACHE_SECS:
            return dict(_capacity_cache_result)
    result = _fetch_capacity(pw)
    with _capacity_cache_lock:
        _capacity_cache_result = dict(result)
        _capacity_cache_ts     = time.time()
    return result


def _invalidate_all_caches() -> None:
    """Call on activate events — stake state and capacity both changed."""
    _invalidate_assess_cache()
    _invalidate_capacity_cache()


def _assess_state_cached(tip: int, pw: str, force: bool = False) -> dict:
    global _assess_cache_result, _assess_cache_ts
    with _assess_cache_lock:
        age = time.time() - _assess_cache_ts
        if not force and _assess_cache_result and age < _ASSESS_CACHE_SECS:
            _log(f"[assess] cache hit (age={age:.0f}s)")
            return dict(_assess_cache_result)
    result = _assess_state(tip, pw)
    with _assess_cache_lock:
        _assess_cache_result = dict(result)
        _assess_cache_ts     = time.time()
    return result


def _assess_state(tip: int, pw: str) -> dict:
    """
    Fetch stake-info for all provisioners via operator wallet (--format json).

    Provisioner states (protocol-accurate):
      inactive  — no stake (stake_dusk < 1000)
      seeded    — eligibility_epoch >= current_epoch + 2  (ta=2)
      maturing  — eligibility_epoch == current_epoch + 1  (ta=1)
      active    — eligibility_epoch <= current_epoch       (ta=0)
    """
    op = OPERATOR_ADDRESS()
    nodes_by_idx = {}

    r = operator_cmd(f"stake-info --operator {op} --format json",
                     timeout=30, password=pw)
    raw = (r.get("stdout") or r.get("stderr") or "").strip()
    si_list = []
    si_epoch = None
    si_remaining_blocks = None
    if raw.startswith("{") or raw.startswith("["):
        try:
            obj = json.loads(raw)
            if isinstance(obj, list):
                si_list = obj
            elif isinstance(obj, dict):
                si_list             = obj.get("provisioners") or [obj]
                si_epoch            = obj.get("epoch")
                si_remaining_blocks = obj.get("remaining_blocks")
        except Exception:
            pass

    current_epoch = si_epoch  # from stake-info response

    for idx in NODE_INDICES:
        addr      = cfg(f"prov_{idx}_address") or ""
        entry     = next((e for e in si_list if e.get("account") == addr), None)

        stake_lux  = entry.get("stake")             if entry else None
        locked_lux = entry.get("locked")            if entry else None
        reward_lux = entry.get("reward")            if entry else None
        eligblk    = entry.get("eligibility_block") if entry else None
        eligepoch  = entry.get("eligibility_epoch") if entry else None
        si_status  = (entry.get("status") or "Inactive").lower() if entry else "inactive"

        stake_dusk  = stake_lux  / 1e9 if stake_lux  is not None else 0.0
        locked_dusk = locked_lux / 1e9 if locked_lux is not None else 0.0
        reward_dusk = reward_lux / 1e9 if reward_lux is not None else 0.0

        MIN_STAKE_DUSK = 1000.0

        if stake_dusk < MIN_STAKE_DUSK:
            status, ta = "inactive", None
        elif eligepoch is not None and current_epoch is not None:
            diff = eligepoch - current_epoch
            if diff <= 0:   status, ta = "active",   0
            elif diff == 1: status, ta = "maturing",  1
            else:           status, ta = "seeded",    2
        else:
            if si_status == "active":
                status, ta = "active", 0
            elif si_status in ("maturing", "seeded"):
                if eligblk is not None and tip > 0:
                    blks_away = eligblk - tip
                    ta_calc = max(0, -(-blks_away // EPOCH_BLOCKS))
                    if ta_calc <= 0:   status, ta = "active",  0
                    elif ta_calc == 1: status, ta = "maturing", 1
                    else:              status, ta = "seeded",   2
                else:
                    status = si_status
                    ta     = 1 if si_status == "maturing" else 2
            else:
                status, ta = "inactive", None

        si_account = entry.get("account", "") if entry else ""
        node = {
            "idx":               idx,
            "address":           addr or si_account,
            "staking_address":   si_account,
            "status":            status,
            "ta":                ta,
            "stake_dusk":        stake_dusk,
            "locked_dusk":       locked_dusk,
            "reward_dusk":       reward_dusk,
            "eligibility_block": eligblk,
            "eligibility_epoch": eligepoch,
            "amount_dusk":       stake_dusk,
            "slashed_dusk":      locked_dusk,
            "rewards_dusk":      reward_dusk,
            "has_stake":         stake_dusk >= MIN_STAKE_DUSK,
        }
        nodes_by_idx[idx] = node
        with _stake_cache_lock:
            _stake_cache[idx] = {
                **node, "ok": r["ok"], "cached": False,
                "si_stake": stake_lux, "si_locked": locked_lux,
                "si_reward": reward_lux, "si_status": si_status,
                "si_eligblk": eligblk, "si_eligepoch": eligepoch,
            }

    active   = [n for n in nodes_by_idx.values() if n["status"] == "active"]
    maturing = [n for n in nodes_by_idx.values() if n["status"] in ("maturing", "seeded")]
    inactive = [n for n in nodes_by_idx.values() if n["status"] == "inactive"]
    label    = f"A:{len(active)} M:{len(maturing)} I:{len(inactive)}"

    # Record snapshot for history tab (non-blocking, best-effort)
    try:
        real_tip = tip
        if not real_tip:
            try:
                from .nodes import get_heights
                hs = get_heights()
                real_tip = max((v for v in hs.values() if v), default=0)
            except Exception:
                pass
        _record_snapshot(real_tip, nodes_by_idx)
    except Exception:
        pass

    return {
        "active": active, "maturing": maturing, "inactive": inactive,
        "by_idx": nodes_by_idx, "label": label,
        "epoch": si_epoch, "remaining_blocks": si_remaining_blocks,
    }


def _fetch_capacity(pw: str) -> dict:
    """Fetch operator capacity from substrate. Returns DUSK values."""
    op  = OPERATOR_ADDRESS()
    r   = operator_cmd(f"substrate capacity --operator {op} --format json",
                       timeout=30, password=pw)
    raw = (r.get("stdout") or r.get("stderr") or "").strip()
    cap = {}
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
            cap["active_current"] = int(obj.get("current_eligibility", obj.get("active_current", 0))) / 1e9
            cap["active_maximum"] = int(obj.get("max_eligibility",     obj.get("active_maximum", 0))) / 1e9
            cap["locked_current"] = int(obj.get("current_stake",       obj.get("locked_current", 0))) / 1e9
            cap["locked_maximum"] = int(obj.get("max_stake",           obj.get("locked_maximum", 0))) / 1e9
        except Exception:
            pass
    return {"active_current": 0.0, "active_maximum": 0.0,
            "locked_current": 0.0, "locked_maximum": 0.0, **cap}


def _max_topup_active(cap: dict) -> float:
    # All math in integer LUX (1 DUSK = exactly 1_000_000_000 LUX).
    # Contract computes locked_added = topup_lux // 10 (integer floor).
    # Largest topup where locked_added == headroom exactly:
    #   topup_lux = headroom_lux * 10 + 9
    # because floor((headroom*10+9) / 10) == headroom
    locked_max_lux = int(round(cap.get("locked_maximum", 0.0) * 1e9))
    locked_cur_lux = int(round(cap.get("locked_current", 0.0) * 1e9))
    headroom_lux   = max(0, locked_max_lux - locked_cur_lux)
    if headroom_lux <= 0 or TOPUP_SLASH_RATE <= 0:
        return 0.0
    max_topup_lux = headroom_lux * 10 + 9
    return max_topup_lux / 1e9


def _stake_headroom(cap: dict) -> float:
    return max(0.0, cap.get("locked_maximum", 0.0) - cap.get("locked_current", 0.0))


def compute_operator_total(state: dict, pool_dusk: float = 0.0) -> dict:
    """Sum the operator's redistributable DUSK across all provisioners + pool.

    "Redistributable" means: DUSK that would return to the operator's pool if
    all provisioners were liquidated + terminated. This includes:
      - active stake     (returns on liquidate)
      - locked stake     (returns on liquidate; locked is collateral, not slashed)
      - maturing stake   (same value as active; the maturing label only affects
                          when it becomes eligible, not the amount)
      - rewards          (returns on terminate)
      - pool balance     (not yet allocated)

    Inactive provisioners contribute their locked/rewards but not stake_dusk
    (which is < MIN_STAKE_DUSK noise for inactive nodes).

    Used by heal.check_threshold_and_trigger to compute an operator-relative
    target_master rather than anchoring the threshold to active_maximum, which
    would produce unreachable targets when operator deposits are below the
    protocol cap.

    Returns a breakdown dict for telemetry/debug:
      {
        "total_dusk":     float,   # sum of all components below
        "active_stake":   float,   # stake on all active provisioners
        "maturing_stake": float,   # stake on all maturing/seeded provisioners
        "locked":         float,   # sum of locked across all provisioners
        "rewards":        float,   # sum of accumulated rewards
        "pool":           float,   # current pool balance
      }
    """
    nodes = state.get("by_idx", {}) if isinstance(state, dict) else {}

    active_stake   = 0.0
    maturing_stake = 0.0
    locked         = 0.0
    rewards        = 0.0

    for n in nodes.values():
        status = n.get("status", "inactive")
        stake  = float(n.get("stake_dusk", 0.0) or 0.0)
        if status == "active":
            active_stake += stake
        elif status in ("maturing", "seeded"):
            maturing_stake += stake
        # inactive: stake_dusk is < MIN_STAKE_DUSK noise, skip
        locked  += float(n.get("locked_dusk", 0.0) or 0.0)
        rewards += float(n.get("reward_dusk", 0.0) or 0.0)

    pool  = float(pool_dusk or 0.0)
    total = active_stake + maturing_stake + locked + rewards + pool

    return {
        "total_dusk":     round(total,          4),
        "active_stake":   round(active_stake,   4),
        "maturing_stake": round(maturing_stake, 4),
        "locked":         round(locked,         4),
        "rewards":        round(rewards,        4),
        "pool":           round(pool,           4),
    }


# ── parse_stake_info (legacy text parser) ─────────────────────────────────────
def parse_stake_info(output: str, current_tip: int = 0) -> dict:
    result = {
        "has_stake": False, "amount_dusk": 0.0, "slashed_dusk": 0.0,
        "transitions": None, "active_block": None, "active_epoch": None,
        "status": "inactive", "status_label": "inactive (0 trans)",
        "staking_address": "", "rewards_dusk": 0.0, "raw": output,
    }
    if not output:
        return result
    output = re.sub(r'\x1b\[[\d;?]*[A-Za-z]', '', output)
    output = re.sub(r'\x1b[()][A-Z0-9]', '', output)

    m = re.search(r"Staking address:\s*([A-Za-z0-9]{40,})", output)
    if m:
        result["staking_address"] = m.group(1).strip()

    if "A stake does not exist" in output:
        return result

    m = re.search(r"Eligible stake:\s*([\d,]+(?:\.\d+)?)\s*DUSK", output)
    if m:
        result["has_stake"]   = True
        result["amount_dusk"] = float(m.group(1).replace(",", ""))
    if not result["has_stake"]:
        return result

    m2 = re.search(r"Reclaimable slashed stake:\s*([\d,]+(?:\.\d+)?)\s*DUSK", output)
    if m2:
        result["slashed_dusk"] = float(m2.group(1).replace(",", ""))

    m_rew = re.search(r"Accumulated rewards:\s*([\d,]+(?:\.\d+)?)\s*DUSK", output)
    if m_rew:
        result["rewards_dusk"] = float(m_rew.group(1).replace(",", ""))

    m_block = re.search(r"Stake active from block\s*#?(\d+).*?Epoch\s+(\d+)", output, re.IGNORECASE)
    if m_block:
        result["active_block"] = int(m_block.group(1))
        result["active_epoch"] = int(m_block.group(2))

    if result["active_epoch"] is not None and current_tip > 0:
        stake_epoch   = result["active_epoch"] - 2
        current_epoch = current_tip // EPOCH_BLOCKS + 1
        result["transitions"] = max(0, current_epoch - stake_epoch)
    else:
        m3 = re.search(r"(?:epoch\s+transitions?|counter)[\s:]+(\d+)", output, re.IGNORECASE)
        if m3:
            result["transitions"] = int(m3.group(1))

    t = result["transitions"]
    if result["active_block"] is not None and current_tip > 0:
        if current_tip >= result["active_block"]:
            result["status"] = "active"
        else:
            stake_epoch   = result["active_epoch"] - 2
            current_epoch = current_tip // EPOCH_BLOCKS + 1
            seen = max(0, current_epoch - stake_epoch)
            result["transitions"] = seen
            result["status"] = "seeded" if seen == 0 else "maturing"
    elif t is not None:
        result["status"] = "seeded" if t == 0 else ("maturing" if t == 1 else "active")
    else:
        result["status"] = "seeded"

    t, s = result["transitions"], result["status"]
    if s == "active":    result["status_label"] = f"active ({t if t is not None else 2}+ trans)"
    elif s == "maturing": result["status_label"] = f"maturing ({t if t is not None else 1} trans)"
    elif s == "seeded":  result["status_label"] = f"seeded ({t if t is not None else 0} trans)"
    else:                result["status_label"] = f"inactive ({t if t is not None else 0} trans)"
    return result
