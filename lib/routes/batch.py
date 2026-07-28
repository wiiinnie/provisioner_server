"""
routes/batch.py — [batch] transaction-batch endpoints.

Wraps the sozu-wallet batch commands (see sozu-wallet README):
  batch                        show queue ({wallet-dir}/batch.json)
  batch propagate              send all queued txs together → same-block execution
  batch remove                 drop the LAST queued tx (CLI supports last-only)
  batch clear                  wipe the queue

Queueing itself happens in the existing action endpoints (allocate_stake,
deactivate_stake, liquidate, terminate, recycle) via their `batch: true`
body flag, which appends --batch to the pool command.

CAUTION (surfaced in the dashboard panel): queued txs carry nonces assigned
at queue time. Any other operator-wallet tx propagated while the batch is
pending — rotation automation, deposit race, un-batched manual actions —
invalidates the whole batch.
"""
import json

from flask import Blueprint, jsonify, request

from .. import batchq
from ..wallet import operator_cmd, get_password

bp = Blueprint("batch", __name__)


def _parse_cli_json(stdout: str):
    try:
        return json.loads(stdout)
    except Exception:
        return None


@bp.route("/api/batch", methods=["GET", "POST"])
def batch_show():
    """Batch queue contents.

    ?meta=1 → sidecar metadata only (fast, no wallet CLI call) — used by the
    nav chip. Without it, also runs `batch` for the CLI's ground-truth view.
    """
    meta = batchq.entries()
    if request.args.get("meta"):
        return jsonify({"ok": True, "entries": meta, "count": len(meta)})

    r      = operator_cmd("batch --format json", timeout=30, password=get_password())
    parsed = _parse_cli_json(r.get("stdout", "")) if r.get("ok") else None
    # Prefer the CLI's count when its output parses to a list — the sidecar
    # misses anything queued outside the dashboard.
    count = len(parsed) if isinstance(parsed, list) else len(meta)
    return jsonify({
        "ok": r.get("ok", False), "entries": meta, "count": count,
        "cli_stdout": r.get("stdout", ""), "cli_stderr": r.get("stderr", ""),
        "cli_parsed": parsed, "duration_ms": r.get("duration_ms", 0),
    })


@bp.route("/api/batch/propagate", methods=["POST"])
def batch_propagate():
    r = operator_cmd("batch propagate --skip-confirmation --format json",
                     timeout=180, password=get_password())
    if r.get("ok"):
        batchq.clear()
    return jsonify({"ok": r.get("ok", False), "stdout": r.get("stdout", ""),
                    "stderr": r.get("stderr", ""),
                    "duration_ms": r.get("duration_ms", 0)})


@bp.route("/api/batch/remove_last", methods=["POST"])
def batch_remove_last():
    r = operator_cmd("batch remove", timeout=30, password=get_password())
    if r.get("ok"):
        batchq.pop_last()
    return jsonify({"ok": r.get("ok", False), "stdout": r.get("stdout", ""),
                    "stderr": r.get("stderr", ""),
                    "duration_ms": r.get("duration_ms", 0)})


@bp.route("/api/batch/clear", methods=["POST"])
def batch_clear():
    r = operator_cmd("batch clear", timeout=30, password=get_password())
    if r.get("ok"):
        batchq.clear()
    return jsonify({"ok": r.get("ok", False), "stdout": r.get("stdout", ""),
                    "stderr": r.get("stderr", ""),
                    "duration_ms": r.get("duration_ms", 0)})
