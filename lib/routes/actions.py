"""
routes/actions.py — Actions tab endpoints.

Buttons: list provisioners · stake activate · deactivate stake · liquidate ·
         terminate · liquidate & terminate · add provisioner · remove provisioner
"""
import json
import time
from flask import Blueprint, jsonify, request

from ..config import (
    _NET, OPERATOR_ADDRESS, NODE_INDICES, RUSK_VERSION, GRAPHQL_URL,
    cfg, _load_config, _save_config,
)
from ..wallet import operator_cmd, wallet_cmd, get_password, WALLET_PATH
from ..assess import (
    parse_stake_info, _fetch_capacity, _stake_headroom,
    _stake_cache, _stake_cache_lock,
)

bp = Blueprint("actions", __name__)

# Imported lazily from rues module to avoid circular import at load time
def _get_event_log():
    from ..rues import _event_log, _event_log_lock, _wait_for_event, _CONFIRM_SECS
    return _event_log, _event_log_lock, _wait_for_event, _CONFIRM_SECS

def _wait_next_block(known_tip: int, timeout_s: int = 60) -> bool:
    import urllib.request as _urw, json as _jw
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            q   = '{ block(height: -1) { header { height } } }'
            req = _urw.Request(GRAPHQL_URL, data=q.encode(),
                               headers={"rusk-version": RUSK_VERSION,
                                        "Content-Type": "application/graphql"},
                               method="POST")
            with _urw.urlopen(req, timeout=6) as r:
                h = int(_jw.loads(r.read()).get("block", {}).get("header", {}).get("height", 0))
            if h > known_tip:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _parse_profiles_addresses(output: str) -> dict:
    import re as _re
    result, current_profile = {}, None
    for line in output.splitlines():
        m = _re.match(r'\s*Profile\s+(\d+)', line)
        if m:
            current_profile = int(m.group(1)) - 1
        m2 = _re.match(r'\s*Public account\s+-\s+([A-Za-z0-9]{40,})', line)
        if m2 and current_profile is not None:
            result[current_profile] = m2.group(1).strip()
    return result


@bp.route("/api/provisioner/list", methods=["GET", "POST"])
def provisioner_list():
    pw = (request.get_json(silent=True) or {}).get("password", request.args.get("password", ""))
    return jsonify(wallet_cmd("profiles", timeout=15, password=pw))


@bp.route("/api/provisioner/addresses", methods=["GET", "POST"])
def provisioner_addresses():
    pw = (request.get_json(silent=True) or {}).get("password", get_password())

    # Single stake-info call returns provisioners in index order (0, 1, 2)
    # Try assess cache first; fall back to direct stake-info parse by position
    addrs = {}
    try:
        from .assess import _assess_state_cached
        st    = _assess_state_cached(0, pw)
        nodes = st.get("by_idx", {})
        for idx in NODE_INDICES:
            node = nodes.get(idx) or {}
            addrs[str(idx)] = node.get("staking_address", "") or cfg(f"prov_{idx}_address") or ""
    except Exception:
        pass

    # If any address missing, parse stake-info output directly by position
    if not all(addrs.get(str(i)) for i in NODE_INDICES):
        try:
            import json as _json
            from .wallet import operator_cmd as _op, get_password as _gpw
            from .config import _NET
            r = _op(f"{_NET} stake-info --format json", timeout=30, password=pw or _gpw() or "")
            raw = (r.get("stdout") or r.get("stderr") or "").strip()
            if raw.startswith("{"):
                data  = _json.loads(raw)
                provs = data.get("provisioners") or []
                for i, p in enumerate(provs):
                    if i in NODE_INDICES:
                        acct = p.get("account", "")
                        if acct and not addrs.get(str(i)):
                            addrs[str(i)] = acct
        except Exception:
            pass

    # Fill any still-missing from config
    for idx in NODE_INDICES:
        if not addrs.get(str(idx)):
            addrs[str(idx)] = cfg(f"prov_{idx}_address") or ""

    # Persist any newly detected addresses back to config
    try:
        changed = False
        current = _load_config()
        for idx in NODE_INDICES:
            live = addrs.get(str(idx), "")
            if live and live != current.get(f"prov_{idx}_address", ""):
                current[f"prov_{idx}_address"] = live
                changed = True
        if changed:
            _save_config(current)
    except Exception:
        pass

    return jsonify({"ok": True, "addresses": addrs, "operator": OPERATOR_ADDRESS()})


@bp.route("/api/provisioner/add_provisioner", methods=["POST"])
def provisioner_add_provisioner():
    data = request.get_json() or {}
    pw   = data.get("password", "")
    prov = data.get("provisioner_address", "")
    if not prov:
        return jsonify({"ok": False, "stderr": "provisioner_address required"}), 400
    r = operator_cmd(
        f"{_NET} pool add-provisioner --skip-confirmation "
        f"--operator {OPERATOR_ADDRESS()} --provisioner {prov}",
        timeout=90, password=pw)
    return jsonify({"ok": r["ok"], **r})


@bp.route("/api/provisioner/allocate_stake", methods=["POST"])
def provisioner_allocate_stake():
    """Stake activate — sends stake from pool to provisioner."""
    data        = request.get_json() or {}
    pw          = data.get("password", "")
    amount      = data.get("amount_dusk", 0)
    if "provisioner_idx" not in data or not amount:
        return jsonify({"ok": False, "stderr": "provisioner_idx and amount_dusk required"}), 400

    idx         = int(data["provisioner_idx"])
    amount_dusk = float(amount)

    try:
        _pw_guard = get_password()
        if _pw_guard:
            cap    = _fetch_capacity(_pw_guard)
            # Look up current node status from stake cache
            with _stake_cache_lock:
                node_status = (_stake_cache.get(idx) or {}).get("status", "inactive")

            if node_status == "active":
                # Topping up an active node incurs a 10% slash penalty on the
                # locked amount — cap by slash headroom / SLASH_RATE
                SLASH_RATE = 0.10
                slash_hdroom = max(0.0, cap.get("locked_maximum", 0.0) - cap.get("locked_current", 0.0))
                max_allowed  = round(slash_hdroom / SLASH_RATE, 4) if SLASH_RATE > 0 else 0.0
                if amount_dusk > max_allowed:
                    return jsonify({
                        "ok": False,
                        "stderr": (
                            f"Slash capacity exceeded for active node prov[{idx}]: "
                            f"locked={cap['locked_current']:,.0f}/{cap['locked_maximum']:,.0f} DUSK, "
                            f"max top-up={max_allowed:,.2f} DUSK (10% slash rate), "
                            f"requested={amount_dusk:,.2f} DUSK."
                        ),
                    }), 400
            else:
                # Inactive/seeded/maturing — no slash penalty, limit is active_maximum
                active_available = max(0.0, cap.get("active_maximum", 0.0) - cap.get("active_current", 0.0))
                if amount_dusk > active_available:
                    return jsonify({
                        "ok": False,
                        "stderr": (
                            f"Operator active stake capacity exceeded for prov[{idx}] ({node_status}): "
                            f"active={cap['active_current']:,.0f}/{cap['active_maximum']:,.0f} DUSK, "
                            f"available={active_available:,.2f} DUSK, "
                            f"requested={amount_dusk:,.2f} DUSK."
                        ),
                    }), 400
    except Exception:
        pass

    amount_lux = int(amount_dusk * 1_000_000_000)
    prov_addr  = cfg(f"prov_{idx}_address") or ""
    if not prov_addr:
        with _stake_cache_lock:
            prov_addr = (_stake_cache.get(idx) or {}).get("staking_address", "")
    if not prov_addr:
        return jsonify({"ok": False,
                        "stderr": f"Provisioner address for prov[{idx}] not found. "
                                  f"Set prov_{idx}_address in config."}), 400

    r = operator_cmd(
        f"{_NET} pool stake-activate --skip-confirmation "
        f"--amount {amount_lux} "
        f"--provisioner {prov_addr} "
        f"--provisioner-wallet {WALLET_PATH} "
        f"--provisioner-password '{pw}'",
        timeout=90, password=pw)
    return jsonify({"ok": r["ok"], "amount_lux": amount_lux, "provisioner": prov_addr, **r})


@bp.route("/api/provisioner/deactivate_stake", methods=["POST"])
def provisioner_deactivate_stake():
    data = request.get_json() or {}
    pw   = data.get("password", "")
    prov = data.get("provisioner_address", "")
    if not prov:
        return jsonify({"ok": False, "stderr": "provisioner_address required"}), 400
    r = operator_cmd(f"{_NET} pool stake-deactivate --skip-confirmation --provisioner {prov}",
                     timeout=90, password=pw)
    return jsonify({"ok": r["ok"], **r})



@bp.route("/api/provisioner/liquidate", methods=["POST"])
def provisioner_liquidate():
    data = request.get_json() or {}
    pw   = data.get("password", "")
    prov = data.get("provisioner_address", "")
    if not prov:
        return jsonify({"ok": False, "stderr": "provisioner_address required"}), 400
    r = operator_cmd(f"{_NET} pool liquidate --skip-confirmation --provisioner {prov}",
                     timeout=90, password=pw)
    return jsonify({"ok": r["ok"], "step": "complete", "results": {"liquidate": r}})


@bp.route("/api/provisioner/terminate", methods=["POST"])
def provisioner_terminate():
    data = request.get_json() or {}
    pw   = data.get("password", "")
    prov = data.get("provisioner_address", "")
    if not prov:
        return jsonify({"ok": False, "stderr": "provisioner_address required"}), 400
    r = operator_cmd(f"{_NET} pool terminate --skip-confirmation --provisioner {prov}",
                     timeout=90, password=pw)
    return jsonify({"ok": r["ok"], "step": "complete", "results": {"terminate": r}})


@bp.route("/api/provisioner/remove_provisioner", methods=["POST"])
def provisioner_remove_provisioner():
    data = request.get_json() or {}
    pw   = data.get("password", "")
    prov = data.get("provisioner_address", "")
    op   = data.get("operator_address", OPERATOR_ADDRESS())
    idx  = data.get("provisioner_idx")
    if not prov or not op:
        return jsonify({"ok": False, "stderr": "provisioner_address and operator_address required"}), 400

    results, status, has_stake = {}, None, False
    if idx is not None:
        r_info    = wallet_cmd(f"--profile-idx {idx} stake-info", timeout=20, password=pw)
        info      = parse_stake_info(r_info.get("stdout", "") + r_info.get("stderr", ""))
        status    = info.get("status", "inactive")
        has_stake = info.get("has_stake", False)
        results["stake_info"] = {"status": status, "has_stake": has_stake}

    if status == "active":
        _tip_snap = 0
        try:
            import urllib.request as _urm, json as _jm
            _qm  = '{ block(height: -1) { header { height } } }'
            _req = _urm.Request(GRAPHQL_URL, data=_qm.encode(),
                                headers={"rusk-version": RUSK_VERSION,
                                         "Content-Type": "application/graphql"}, method="POST")
            with _urm.urlopen(_req, timeout=6) as _rr:
                _tip_snap = int(_jm.loads(_rr.read()).get("block", {}).get("header", {}).get("height", 0))
        except Exception:
            pass
        r1 = operator_cmd(f"{_NET} pool liquidate --skip-confirmation --provisioner {prov}", timeout=90, password=pw)
        results["liquidate"] = r1
        if not r1["ok"]:
            return jsonify({"ok": False, "step": "liquidate", "results": results})
        _wait_next_block(_tip_snap, timeout_s=60)
        r2 = operator_cmd(f"{_NET} pool terminate --skip-confirmation --provisioner {prov}", timeout=90, password=pw)
        results["terminate"] = r2
        if not r2["ok"]:
            return jsonify({"ok": False, "step": "terminate", "results": results})
        time.sleep(5)

    elif has_stake and status in ("maturing", "seeded", "inactive"):
        r1 = operator_cmd(f"{_NET} pool stake-deactivate --skip-confirmation --provisioner {prov}", timeout=90, password=pw)
        results["deactivate"] = r1
        if not r1["ok"]:
            return jsonify({"ok": False, "step": "deactivate", "results": results})
        time.sleep(8)

    r5 = operator_cmd(
        f"{_NET} pool remove-provisioner --skip-confirmation --provisioner {prov}",
        timeout=90, password=pw)
    results["remove"] = r5
    return jsonify({"ok": r5["ok"], "step": "complete",
                    "pre_step": status or "unknown", "results": results})
