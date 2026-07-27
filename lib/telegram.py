# lib/telegram.py — rate-limited TG alert helpers
import os
import time
import threading
import requests

from .config import _log, cfg

# ── Rate-limit per alert key ──────────────────────────────────────────────────
# Each alert key has its own cooldown window. If an alert fires within the
# cooldown, it's suppressed silently.
_COOLDOWN_SEC = 30 * 60   # 30 min default cooldown between repeat alerts
_last_sent: dict = {}
_lock = threading.Lock()


def _can_send(alert_key: str) -> bool:
    if not alert_key:
        return True   # untagged alerts always send
    with _lock:
        last = _last_sent.get(alert_key, 0)
        now  = time.time()
        if now - last < _COOLDOWN_SEC:
            return False
        _last_sent[alert_key] = now
    return True


def _is_enabled(alert_key: str) -> bool:
    """[notification_rework] Check whether this alert type is enabled in user config.

    Reads config["notification_settings"][alert_key]. Defaults to True if
    the key is not present (backward-compat with existing installs).

    Normalizes dynamic-suffix keys: an alert_key with a trailing numeric
    suffix ("some_alert_3") is looked up under its base key ("some_alert").
    """
    if not alert_key:
        return True   # untagged alerts (e.g. alert_info) always send
    settings = cfg("notification_settings") or {}
    base_key = alert_key
    if "_" in alert_key:
        parts = alert_key.rsplit("_", 1)
        if parts[1].isdigit():
            base_key = parts[0]
    return settings.get(base_key, True)


def reset_alert(alert_key: str) -> None:
    """Call when the underlying condition clears, so the next crossing re-alerts."""
    with _lock:
        _last_sent.pop(alert_key, None)


def send(message: str, alert_key: str = "", parse_mode: str = "HTML") -> bool:
    token = cfg("telegram_bot_token") or ""
    chat  = cfg("telegram_chat_id")   or ""
    if not token or not chat:
        return False
    if not _is_enabled(alert_key):   # [notification_rework] per-alert config gate
        return False
    if not _can_send(alert_key):
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id":    chat,
                "text":       message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=8,
        )
        if r.status_code != 200:
            _log(f"[telegram] send failed: {r.status_code} {r.text[:120]}")
            return False
        return True
    except Exception as e:
        _log(f"[telegram] send error: {e}")
        return False


def send_async(message: str, alert_key: str = "", parse_mode: str = "HTML") -> None:
    threading.Thread(
        target=send, args=(message, alert_key, parse_mode), daemon=True
    ).start()


# ── Master alerts ─────────────────────────────────────────────────────────────

def alert_master_below_threshold(
        prov_idx: int,
        stake_dusk: float,
        alert_threshold_dusk: float,
        alert_threshold_pct: float,
        target_master_dusk: float,
) -> None:
    """Fired when master stake drops below the ALERT threshold. Refunding the
    master is a manual operation (dashboard \u2192 redistribute stake)."""
    msg = (
        f"\u26A0\uFE0F <b>SOZU \u2014 Master Alert</b>\n\n"
        f"prov{prov_idx} stake has crossed the alert threshold.\n\n"
        f"<b>Current stake:</b> {stake_dusk:,.2f} DUSK\n"
        f"<b>Alert threshold ({alert_threshold_pct:.0f}% of target):</b> "
        f"{alert_threshold_dusk:,.2f} DUSK\n"
        f"<b>Target master:</b> {target_master_dusk:,.0f} DUSK\n\n"
        f"There is no automated heal \u2014 if the master needs re-funding, "
        f"run a manual redistribution from the dashboard."
    )
    send_async(msg, alert_key="master_below_alert_threshold")


def alert_rotation_failed(reason: str) -> None:
    """Send rotation failure alert."""
    msg = (
        f"🔴 <b>SOZU — Rotation Failed</b>\n\n"
        f"{reason}"
    )
    send_async(msg, alert_key="rotation_failed")


def alert_rotation_success(
        cur_epoch: int,
        node_summaries: list,
        pool_dusk: float,
        total_dusk: float,
) -> None:
    """[notification_rework] Send rotation success summary with node states + totals.

    node_summaries: list of dicts with keys {idx, stake, role, status}.
      role:   "master" | "standby" | "rot_active" | "rot_slave" | "rot_seeded" | "—"
      status: "active" | "1 epoch away" | "2 epochs away" | "inactive" | "unknown"
    """
    lines = [f"✅ <b>SOZU — Rotation Complete</b> (epoch {cur_epoch})", ""]
    for n in node_summaries:
        lines.append(
            f"Node {n['idx']}:  <code>{n['stake']:>13,.0f} DUSK</code> "
            f"· {n['role']} · {n['status']}"
        )
    lines.append("")
    lines.append(f"Pool:   <code>{pool_dusk:>13,.0f} DUSK</code>")
    lines.append(f"Total:  <code>{total_dusk:>13,.0f} DUSK</code>")
    send_async("\n".join(lines), alert_key="rotation_success")


def alert_info(message: str) -> None:
    """Send a plain informational message (no rate limiting)."""
    msg = f"ℹ️ <b>SOZU</b>\n\n{message}"
    send_async(msg)
