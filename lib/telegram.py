"""
telegram.py — Telegram notification helpers for SOZU Dashboard.

Sends alerts via Telegram Bot API using a simple HTTP POST.
No third-party libraries required — uses urllib only.

Config keys:
    telegram_bot_token  — Bot token from @BotFather
    telegram_chat_id    — Chat/group ID to send messages to

Alerts are rate-limited: the same alert key will not fire more than once
per cooldown period (default 1 epoch = 2160 blocks ≈ 5.4 hours).
"""
import json
import threading
import time
import urllib.request as _ur
import urllib.error   as _ue

from .config import cfg, _log

# ── Rate limiting ──────────────────────────────────────────────────────────────
# Maps alert_key → last_sent_timestamp
_alert_last_sent: dict  = {}
_alert_lock              = threading.Lock()

# Cooldown: don't re-send same alert more than once per period (seconds)
ALERT_COOLDOWN_S = 6 * 3600   # 6 hours


def _can_send(alert_key: str) -> bool:
    """Return True if this alert key is not in cooldown."""
    with _alert_lock:
        last = _alert_last_sent.get(alert_key, 0.0)
        if time.time() - last >= ALERT_COOLDOWN_S:
            _alert_last_sent[alert_key] = time.time()
            return True
    return False


def reset_alert(alert_key: str) -> None:
    """Clear cooldown for an alert key (e.g. when condition resolves)."""
    with _alert_lock:
        _alert_last_sent.pop(alert_key, None)


def send(message: str, alert_key: str = "", parse_mode: str = "HTML") -> bool:
    """
    Send a Telegram message.

    Args:
        message:    Text to send. Supports HTML formatting when parse_mode="HTML".
        alert_key:  If set, rate-limits this alert. Empty string = always send.
        parse_mode: "HTML" or "Markdown" or "" for plain text.

    Returns True if sent successfully, False otherwise.
    """
    bot_token = cfg("telegram_bot_token") or ""
    chat_id   = cfg("telegram_chat_id")   or ""

    if not bot_token or not chat_id:
        _log("[telegram] not configured — skipping notification")
        return False

    if alert_key and not _can_send(alert_key):
        _log(f"[telegram] alert '{alert_key}' in cooldown — skipping")
        return False

    url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        data = json.dumps(payload).encode()
        req  = _ur.Request(url, data=data,
                           headers={"Content-Type": "application/json"},
                           method="POST")
        with _ur.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            if resp.get("ok"):
                _log(f"[telegram] sent: {message[:80]}…")
                return True
            _log(f"[telegram] API error: {resp}")
            raise RuntimeError(f"Telegram API error: {resp.get('description', resp)}")
    except _ue.URLError as e:
        _log(f"[telegram] network error: {e}")
        return False
    except Exception as e:
        _log(f"[telegram] error: {e}")
        return False


def send_async(message: str, alert_key: str = "", parse_mode: str = "HTML") -> None:
    """Fire-and-forget version — runs in background thread."""
    threading.Thread(
        target=send,
        args=(message, alert_key, parse_mode),
        daemon=True,
    ).start()


# ── Pre-built alert templates ─────────────────────────────────────────────────

def alert_master_below_threshold(
        prov_idx: int,
        stake_dusk: float,
        threshold_dusk: float,
        threshold_pct: float,
        active_maximum_dusk: float,
) -> None:
    """Send master stake threshold alert."""
    msg = (
        f"⚠️ <b>SOZU — Master Node Alert</b>\n\n"
        f"prov{prov_idx} stake has fallen below the threshold.\n\n"
        f"<b>Current stake:</b> {stake_dusk:,.2f} DUSK\n"
        f"<b>Threshold ({threshold_pct:.0f}% of max):</b> {threshold_dusk:,.2f} DUSK\n"
        f"<b>Max capacity:</b> {active_maximum_dusk:,.0f} DUSK\n\n"
        f"Manual intervention required to restore master stake.\n"
        f"See dashboard for current state."
    )
    send_async(msg, alert_key="master_below_threshold")


def alert_rotation_failed(reason: str) -> None:
    """Send rotation failure alert."""
    msg = (
        f"🔴 <b>SOZU — Rotation Failed</b>\n\n"
        f"{reason}"
    )
    send_async(msg, alert_key="rotation_failed")


def alert_info(message: str) -> None:
    """Send a plain informational message (no rate limiting)."""
    msg = f"ℹ️ <b>SOZU</b>\n\n{message}"
    send_async(msg)
