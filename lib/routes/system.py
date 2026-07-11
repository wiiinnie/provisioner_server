"""
routes/system.py — System, config, nodes, version endpoints.
No rotation. No RUES.
"""
import json
import os
import time
from datetime import datetime
from flask import Blueprint, jsonify, request, send_file

from ..config import (
    _log, NETWORK, CONTRACT_ID, _NODE_STATE_URL, RUSK_VERSION,
    WALLET_BIN, WALLET_PATH, NODE_INDICES, PORT,
    cfg, _cfg, _CONFIG_DEFAULTS, _load_config, _save_config,
)
from ..wallet import run_cmd, wallet_cmd, get_password, _cache_wallet_pw, OPERATOR_WALLET

bp = Blueprint("system", __name__)

# [version_dynamic] SERVER_VERSION constant retired — version comes from git
# metadata via the /api/version endpoint below.


@bp.route("/api/ping")
def ping():
    return jsonify({"ok": True, "ts": datetime.now().isoformat()})


@bp.route("/")
def index():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(os.path.dirname(here))
    import glob
    candidates = sorted(glob.glob(os.path.join(root, "provisioner_dashboard*.html")))
    if not candidates:
        return ("Dashboard HTML not found.", 404)
    return send_file(candidates[-1])


@bp.route("/api/status")
def status():
    binary_ok = run_cmd(f"which {WALLET_BIN}")["ok"]
    from ..wallet import get_password
    return jsonify({
        "binary_found":  binary_ok,
        "wallet_dir":    WALLET_PATH,
        "wallet_dir_ok": os.path.isdir(WALLET_PATH),
        "network":       NETWORK,
        "nodes":         NODE_INDICES,
        "password_set":  bool(get_password()),
        "ts":            datetime.now().isoformat(),
    })


# [redistribute] manual redistribution endpoints
@bp.route("/api/redistribute/status")
def redistribute_status():
    try:
        from ..redistribute import get_status
        return jsonify({"ok": True, **get_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/redistribute/preview")
def redistribute_preview():
    """Preview what a redistribution with the given target would move, using
    current on-chain state. Query param: ?target=<dusk> (default 1,000,000)."""
    try:
        from ..redistribute import _preview_amounts, DEFAULT_TARGET_DUSK
        from ..rotation import _assess, _master_idx, ROTATION_PAIR
        target = float(request.args.get("target", DEFAULT_TARGET_DUSK))
        st    = _assess()
        nodes = st.get("by_idx", {})
        cur_master_idx = _master_idx()
        new_master_idx = next((i for i in (0, 1) if i != cur_master_idx), None)
        rot_active_idx = next((i for i in ROTATION_PAIR
                               if nodes.get(i, {}).get("status") == "active"
                               and nodes.get(i, {}).get("ta") == 0), None)
        if rot_active_idx is None:
            return jsonify({"ok": False, "error": "no active rot_master found"})
        pv = _preview_amounts(nodes, target, rot_active_idx, cur_master_idx)
        return jsonify({
            "ok": True,
            "target": target,
            "cur_master_idx": cur_master_idx,
            "new_master_idx": new_master_idx,
            "rot_active_idx": rot_active_idx,
            "new_master_inactive": nodes.get(new_master_idx, {}).get("status") == "inactive",
            **pv,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/redistribute/arm", methods=["POST"])
def redistribute_arm():
    try:
        from ..redistribute import arm
        data = request.get_json(silent=True) or {}
        target = data.get("target_dusk")
        return jsonify(arm(target))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/redistribute/reset", methods=["POST"])
def redistribute_reset():
    try:
        from ..redistribute import reset
        reset()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/version")
def api_version():
    # [version_dynamic] read git metadata at request time
    import subprocess
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def _git(args, fallback="unknown"):
        try:
            r = subprocess.run(
                ["git"] + args,
                cwd=_repo_root,
                capture_output=True, text=True, timeout=2,
            )
            return r.stdout.strip() or fallback
        except Exception:
            return fallback

    return jsonify({
        "branch":   _git(["rev-parse", "--abbrev-ref", "HEAD"], fallback="main"),
        "sha":      _git(["rev-parse", "--short", "HEAD"]),
        "date":     _git(["log", "-1", "--format=%cd", "--date=format:%Y-%m-%d"]),
        "dirty":    bool(_git(["status", "--porcelain"], fallback="")),
        "network":  NETWORK,
        "node_url": _NODE_STATE_URL,
        "pool":     CONTRACT_ID,
    })


@bp.route("/api/debug/wallet", methods=["POST"])
def debug_wallet():
    data    = request.get_json() or {}
    pw      = data.get("password", "")
    subcmd  = data.get("subcmd", "profiles")
    timeout = data.get("timeout", 20)
    if subcmd == "help":
        result = run_cmd(f"{WALLET_BIN} --help", timeout=10)
    elif subcmd == "help_network":
        result = run_cmd(f"{WALLET_BIN} -w {WALLET_PATH} --help", timeout=10)
    else:
        result = wallet_cmd(subcmd, timeout=timeout, password=pw)
    return jsonify(result)


@bp.route("/api/config", methods=["GET"])
def get_config():
    from ..config import _cfg as _current_cfg
    cfg("network_id")
    safe = {k: v for k, v in _current_cfg.items() if not k.endswith("_sk")}
    return jsonify(safe)


@bp.route("/api/config", methods=["POST"])
def set_config():
    from ..config import _cfg as _current_cfg
    data    = request.get_json() or {}
    current = dict(_current_cfg) if _current_cfg else dict(_CONFIG_DEFAULTS)
    int_keys   = ("network_id","rotation_window","snatch_window","backfill_blocks",
                  "master_idx","gas_limit","gas_price","node_0_ws_port","node_1_ws_port","node_2_ws_port","node_3_ws_port",
                  "sweeper_delay_blocks","max_harvest_deferrals")  # [config_whitelist_fix]
    bool_keys  = ("sweeper_enabled", "deposit_race_paused", "master_heal_enabled",)  # [deposit_race_pause] [config_whitelist_fix]
    float_keys = ("min_deposit_dusk","snatch_min_deposit_dusk","master_threshold_pct", "locked_max_pct", "min_viable_master_dusk",
                  "master_alert_threshold_pct","master_heal_threshold_pct","rotation_floor_pct")  # [config_whitelist_fix]
    str_keys   = ("contract_address","operator_address",
                  "prov_0_address","prov_1_address","prov_2_address","prov_3_address",
                  "node_0_log","node_1_log","node_2_log","node_3_log",
                  "telegram_bot_token","telegram_chat_id","node_state_url")
    for k in int_keys:
        if k in data:
            current[k] = max(100000,int(data[k])) if k=="gas_limit" else int(data[k])
    for k in float_keys:
        if k in data: current[k] = float(data[k])

    # ── Validate rotation_floor_pct: must be in [5, 50] range ────────────────
    # Below 5%: rotation pair underfunded, can't earn meaningfully or rotate.
    # Above 50%: master loses primacy, target_master would be smaller than
    #            rotation pair's own allocation, breaking the threshold logic.
    if "rotation_floor_pct" in current:
        v = float(current["rotation_floor_pct"])
        if v < 5.0 or v > 50.0:
            return jsonify({
                "ok": False,
                "error": f"rotation_floor_pct must be between 5 and 50 (got {v})"
            }), 400

    # ── Validate sweeper_delay_blocks: must be in [10, 500] range ────────────
    # Below 10: sub-cadence delays are rounded up to STATE_CHECK_BLOCKS anyway.
    # Above 500: ~83min delay is impractical, deposits would expire eligibility.
    if "sweeper_delay_blocks" in current:
        v = int(current["sweeper_delay_blocks"])
        if v < 10 or v > 500:
            return jsonify({
                "ok": False,
                "error": f"sweeper_delay_blocks must be between 10 and 500 (got {v})"
            }), 400
    for k in bool_keys:
        if k in data: current[k] = bool(data[k])
    for k in str_keys:
        if k in data: current[k] = str(data[k]).strip()
    # [config_whitelist_fix] dict-typed keys (merge with existing values)
    dict_keys = ("notification_settings",)
    for k in dict_keys:
        if k in data and isinstance(data[k], dict):
            current[k] = {**(current.get(k) or {}), **data[k]}
    _save_config(current)
    safe = {k: v for k, v in current.items() if not k.endswith("_sk")}
    return jsonify({"ok": True, "config": safe})


@bp.route("/api/config/reset", methods=["POST"])
def reset_config():
    from ..config import _cfg as _current_cfg
    _save_config(dict(_CONFIG_DEFAULTS))
    return jsonify({"ok":True,"config":_current_cfg})


@bp.route("/api/nodes/heights", methods=["GET"])
def nodes_heights():
    from ..nodes import get_heights
    return jsonify(get_heights())


@bp.route("/api/telegram/test", methods=["POST"])
def telegram_test():
    try:
        from ..telegram import send
        # [config_whitelist_fix] optional alert_key for per-alert test buttons
        data = request.get_json(silent=True) or {}
        alert_key = (data.get("alert_key") or "").strip()
        if alert_key:
            msg = f"✅ SOZU Dashboard — test message for '{alert_key}'"
        else:
            msg = "✅ SOZU Dashboard — Telegram test message"
        ok = send(msg, alert_key=alert_key)
        if ok:
            return jsonify({"ok": True, "error": None})
        # send() returns False if: no Telegram config / alert disabled / cooldown / API error
        if alert_key:
            err = "send returned False (alert may be disabled in config, or in cooldown)"
        else:
            err = "send returned False (no Telegram config?)"
        return jsonify({"ok": False, "error": err})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/nodes/sync", methods=["GET"])
def nodes_sync():
    import urllib.request as _ur
    from ..nodes import get_heights
    from ..config import GRAPHQL_URL
    network_tip = None
    try:
        q   = "{ block(height: -1) { header { height } } }"
        req = _ur.Request(GRAPHQL_URL, data=q.encode(),
                          headers={"rusk-version": RUSK_VERSION,
                                   "Content-Type": "application/graphql"},
                          method="POST")
        with _ur.urlopen(req, timeout=6) as r:
            p = json.loads(r.read())
        b = p.get("block") or p.get("data",{}).get("block",{})
        network_tip = int(b["header"]["height"])
    except Exception:
        pass
    cached = get_heights()
    results = {}
    for idx in NODE_INDICES:
        h = cached.get(idx)
        results[str(idx)] = {"height":h,"ok":h is not None,"source":"ws" if h else "none"}
    return jsonify({"ok":True,"network_tip":network_tip,"nodes":results})


_SOZU_ADDR_FNS           = frozenset(["liquidate","terminate","stake_deactivate","remove_provisioner"])
_SOZU_AMOUNT_FNS         = frozenset(["sozu_stake","sozu_unstake","sozu_airdrop"])
_SOZU_STAKE_ACTIVATE_FNS = frozenset(["stake_activate"])
_SOZU_DECODE_FNS         = _SOZU_ADDR_FNS | _SOZU_AMOUNT_FNS | _SOZU_STAKE_ACTIVATE_FNS


def _decode_fn_args(fn_name:str, fn_args_b64:str) -> dict:
    if fn_name not in _SOZU_DECODE_FNS or not fn_args_b64: return {}
    import subprocess as _sp, base64 as _b64
    try:
        raw_bytes = _b64.b64decode(fn_args_b64)
        hex_val   = "0x" + raw_bytes.hex()
        url = f"{_NODE_STATE_URL}/on/driver:{CONTRACT_ID}/decode_input_fn:{fn_name}"
        r = _sp.run(["curl","-s","-X","POST",url,"-H",f"rusk-version: {RUSK_VERSION}","-d",hex_val],
                    capture_output=True,text=True,timeout=8)
        result = r.stdout.strip()
        if not result: return {}
        if fn_name in _SOZU_STAKE_ACTIVATE_FNS:
            obj=json.loads(result); bls=(obj.get("keys") or {}).get("account","")
            lux=int(str(obj.get("value","0"))); out={}
            if bls: out["provisioner"]=bls
            if lux: out["amount_lux"]=lux; out["amount_dusk"]=round(lux/1e9,9)
            return out
        if fn_name in _SOZU_AMOUNT_FNS:
            lux=int(result.strip('"'))
            return {"amount_lux":lux,"amount_dusk":round(lux/1e9,9)}
        clean=result.strip('"')
        return {"provisioner":clean} if len(clean)>20 else {}
    except Exception as e:
        _log(f"[decode_fn_args] {fn_name}: {e}"); return {}


@bp.route("/api/decode_fn_args", methods=["POST"])
def api_decode_fn_args():
    data=request.get_json() or {}
    fn_name=data.get("fn_name","").strip(); fn_args=data.get("fn_args","").strip()
    if not fn_name or not fn_args:
        return jsonify({"ok":False,"error":"fn_name and fn_args required"}),400
    result=_decode_fn_args(fn_name,fn_args)
    if not result:
        return jsonify({"ok":False,"error":f"decode_input_fn:{fn_name} returned empty"})
    return jsonify({"ok":True,"fn_name":fn_name,"result":result})


# ── Stake history ─────────────────────────────────────────────────────────────

@bp.route("/api/history/stake", methods=["GET"])
def history_stake():
    from ..assess import get_stake_history
    try:
        max_blocks = int(request.args.get("blocks", 2160))
    except (ValueError, TypeError):
        max_blocks = 2160
    max_blocks = min(max_blocks, 60480)   # cap at 7d (10s/block)
    return jsonify(get_stake_history(max_blocks))


# ── RUES event stream ─────────────────────────────────────────────────────────

@bp.route("/api/rues/status", methods=["GET"])
def rues_status():
    from ..rues import get_status
    return jsonify(get_status())


@bp.route("/api/rues/subscribe", methods=["POST"])
def rues_subscribe():
    from ..rues import subscribe_topic
    data   = request.get_json() or {}
    topic  = data.get("topic", "").strip()
    action = data.get("action", "subscribe")
    if action not in ("subscribe", "unsubscribe"):
        return jsonify({"ok": False, "error": "action must be subscribe or unsubscribe"}), 400
    return jsonify(subscribe_topic(topic, action))


# ── Deposit event log ─────────────────────────────────────────────────────────

@bp.route("/api/events/deposit_log", methods=["GET"])
def deposit_log():
    from ..events import get_deposit_log
    return jsonify(get_deposit_log())


@bp.route("/api/events/deposit_log/clear", methods=["POST"])
def deposit_log_clear():
    from ..events import _deposit_log, _deposit_log_lock
    with _deposit_log_lock:
        _deposit_log.clear()
    return jsonify({"ok": True})


@bp.route("/api/events/race_counters", methods=["GET"])
def race_counters():
    from ..events import get_race_counters
    return jsonify(get_race_counters())


# [deposit_race_pause] pause endpoint
@bp.route("/api/events/deposit_race/pause", methods=["POST"])
def deposit_race_pause():
    """Toggle or set the deposit_race_paused config flag.
    Body: {"paused": true|false}  (optional — omit to toggle)
    Returns: {"ok": true, "paused": <new_state>}
    """
    from ..config import _cfg as _current_cfg
    data = request.get_json(silent=True) or {}
    current = dict(_current_cfg) if _current_cfg else dict(_CONFIG_DEFAULTS)
    if "paused" in data:
        new_state = bool(data["paused"])
    else:
        new_state = not bool(current.get("deposit_race_paused", False))
    current["deposit_race_paused"] = new_state
    _save_config(current)
    _log(f"[deposit-race] {'paused' if new_state else 'resumed'} by user")
    return jsonify({"ok": True, "paused": new_state})





# ── Rotation automation ───────────────────────────────────────────────────────

@bp.route("/api/rotation/status", methods=["GET"])
def rotation_status():
    from ..rotation import get_status
    return jsonify(get_status())


@bp.route("/api/rotation/toggle", methods=["POST"])
def rotation_toggle():
    from ..rotation import toggle_enabled
    from ..wallet import get_password
    data = request.get_json() or {}
    pw   = data.get("password", "") or get_password()
    return jsonify(toggle_enabled(pw))


@bp.route("/api/rotation/reset_error", methods=["POST"])
def rotation_reset_error():
    from ..rotation import reset_error
    reset_error()
    return jsonify({"ok": True})


@bp.route("/api/password/status", methods=["GET"])
def password_status():
    """Reports whether get_password() can produce a non-empty password.
    Used by the frontend to decide whether to show the wallet-password modal.
    With systemd credential loading, the backend has a password from the moment
    the service starts, so the modal is unnecessary in headless mode."""
    from ..wallet import get_password
    pw = get_password()
    return jsonify({"set": bool(pw)})
