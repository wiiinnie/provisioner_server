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


def reset_alert(alert_key: str) -> None:
    """Call when the underlying condition clears, so the next crossing re-alerts."""
    with _lock:
        _last_sent.pop(alert_key, None)


def send(message: str, alert_key: str = "", parse_mode: str = "HTML") -> bool:
    token = cfg("telegram_bot_token") or ""
    chat  = cfg("telegram_chat_id")   or ""
    if not token or not chat:
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


# ── Master-heal alerts ────────────────────────────────────────────────────────

def alert_master_below_threshold(
        prov_idx: int,
        stake_dusk: float,
        alert_threshold_dusk: float,
        alert_threshold_pct: float,
        target_master_dusk: float,
) -> None:
    """Fired when master stake drops below the ALERT threshold. Message wording
    adapts to current heal state.
    """
    heal_status_line = (
        "Heal will trigger automatically if stake drops further to the heal "
        "threshold. No manual action needed."
    )
    try:
        from .heal import _heal_state, _heal_state_lock
        with _heal_state_lock:
            hs = dict(_heal_state)
        state = hs.get("state", "idle")
        standby_idx = hs.get("standby_idx")

        if state == "awaiting_n":
            heal_status_line = (
                f"\U0001F527 <b>Heal already scheduled.</b> Stake is below the "
                f"heal threshold; standby prov{standby_idx} will be seeded at "
                f"the next rotation window. No manual action needed."
            )
        elif state == "seeded":
            defer = hs.get("deferral_count", 0)
            extra = ""
            if defer:
                extra = f" Harvest deferred {defer}\u00d7 (we are unstake target)."
            heal_status_line = (
                f"\U0001F331 <b>Heal in progress.</b> Standby prov{standby_idx} "
                f"already seeded. Harvest runs at next rotation window.{extra}"
            )
        elif state == "harvesting":
            heal_status_line = (
                f"\u26A1 <b>Heal harvesting.</b> Liquidation + new master "
                f"funding in progress."
            )
        elif state == "completing":
            heal_status_line = (
                f"\u2705 <b>Heal nearly complete.</b> Role swap at next epoch boundary."
            )
        elif state == "failed":
            heal_status_line = (
                f"\U0001F534 <b>Heal in FAILED state.</b> Will retry on next "
                f"threshold check. Manual reset may be needed."
            )
    except Exception:
        pass

    msg = (
        f"\u26A0\uFE0F <b>SOZU \u2014 Master Alert</b>\n\n"
        f"prov{prov_idx} stake has crossed the alert threshold.\n\n"
        f"<b>Current stake:</b> {stake_dusk:,.2f} DUSK\n"
        f"<b>Alert threshold ({alert_threshold_pct:.0f}% of target):</b> "
        f"{alert_threshold_dusk:,.2f} DUSK\n"
        f"<b>Target master:</b> {target_master_dusk:,.0f} DUSK\n\n"
        f"{heal_status_line}"
    )
    send_async(msg, alert_key="master_below_alert_threshold")

def alert_heal_triggered(
        prov_idx: int,
        standby_idx: int,
        stake_dusk: float,
        heal_threshold_dusk: float,
        heal_threshold_pct: float,
) -> None:
    """Fired when heal transitions IDLE → AWAITING_N (master below heal threshold)."""
    msg = (
        f"🔧 <b>SOZU — Heal Triggered</b>\n\n"
        f"Master prov{prov_idx} stake {stake_dusk:,.2f} DUSK has fallen below "
        f"the heal threshold {heal_threshold_dusk:,.2f} DUSK "
        f"({heal_threshold_pct:.0f}% of target).\n\n"
        f"Standby prov{standby_idx} will be seeded at the next rotation window. "
        f"The full heal cycle takes ~3 epochs."
    )
    send_async(msg, alert_key="heal_triggered")


def alert_heal_seeded(prov_idx: int, standby_idx: int) -> None:
    """Standby seeded with 1k DUSK (AWAITING_N → SEEDED)."""
    msg = (
        f"🌱 <b>SOZU — Heal Progress</b>\n\n"
        f"Standby prov{standby_idx} has been seeded. Maturing over "
        f"the next epoch.\n\n"
        f"Heal will run the full harvest (liquidate prov{prov_idx}, "
        f"fund new master) at the next rotation window."
    )
    send_async(msg, alert_key="heal_seeded")


def alert_heal_deferred(
        standby_idx: int,
        deferral_count: int,
        max_deferrals: int,
) -> None:
    """Harvest deferred because we're the substrate unstake target."""
    msg = (
        f"⏸️ <b>SOZU — Heal Deferred</b>\n\n"
        f"We are the substrate unstake target this epoch. Harvest "
        f"postponed to avoid liquidating while unstakes are hitting us.\n\n"
        f"<b>Deferrals:</b> {deferral_count}/{max_deferrals}\n\n"
        f"Heal will force-run after {max_deferrals} deferrals if we remain "
        f"the target."
    )
    send_async(msg, alert_key=f"heal_deferred_{deferral_count}")


def alert_heal_force_run(deferral_count: int) -> None:
    """Force-running harvest after max_deferrals cap hit."""
    msg = (
        f"⚡ <b>SOZU — Heal Force-Running</b>\n\n"
        f"Deferred {deferral_count}× — force-running harvest now to avoid "
        f"infinite deferral. Some unstakes may hit mid-harvest."
    )
    send_async(msg, alert_key="heal_force_run")


def alert_heal_harvest_complete(
        old_master_idx: int,
        new_master_idx: int,
        new_master_stake_dusk: float,
) -> None:
    """Harvest complete (COMPLETING state). Role swap happens at next epoch boundary."""
    msg = (
        f"✅ <b>SOZU — Heal Harvest Complete</b>\n\n"
        f"Harvest finished. Role swap will take effect at the next epoch boundary.\n\n"
        f"<b>Old master:</b> prov{old_master_idx}\n"
        f"<b>New master:</b> prov{new_master_idx} with "
        f"{new_master_stake_dusk:,.0f} DUSK"
    )
    send_async(msg, alert_key="heal_harvest_complete")


def alert_heal_complete(new_master_idx: int, new_master_stake_dusk: float) -> None:
    """Role swap done, heal returned to IDLE."""
    msg = (
        f"🎯 <b>SOZU — Heal Complete</b>\n\n"
        f"Master role swapped to prov{new_master_idx} "
        f"({new_master_stake_dusk:,.0f} DUSK). Heal idle."
    )
    send_async(msg, alert_key="heal_complete")


def alert_heal_failed(reason: str) -> None:
    """Heal step failed — state is FAILED, will retry on next threshold check."""
    msg = (
        f"🔴 <b>SOZU — Heal Failed</b>\n\n"
        f"Heal step failed: {reason}\n\n"
        f"Heal will retry when the master threshold is next crossed."
    )
    send_async(msg, alert_key="heal_failed")


def alert_insufficient_operator_stake(
        operator_total: float,
        target_master: float,
        min_viable: float,
        breakdown: dict,
) -> None:
    """Heal threshold tripped, but achievable target_master is below
    min_viable_master_dusk. Operator needs to deposit more DUSK before heal
    can produce a viable master.

    Rate-limited to once per cooldown window via the standard alert_key
    mechanism. Operator gets one alert per ~30min while the condition holds.
    """
    msg = (
        f"⚠️ <b>SOZU — Heal Blocked: Insufficient Operator Stake</b>\n\n"
        f"Master is below the heal threshold, but heal cannot produce a "
        f"viable master with current operator deposits.\n\n"
        f"<b>Operator total:</b> {operator_total:,.2f} DUSK\n"
        f"  ↳ active stake:    {breakdown.get('active_stake', 0):,.2f}\n"
        f"  ↳ maturing stake:  {breakdown.get('maturing_stake', 0):,.2f}\n"
        f"  ↳ locked:          {breakdown.get('locked', 0):,.2f}\n"
        f"  ↳ rewards:         {breakdown.get('rewards', 0):,.2f}\n"
        f"  ↳ pool:            {breakdown.get('pool', 0):,.2f}\n\n"
        f"<b>Achievable master:</b> {target_master:,.2f} DUSK\n"
        f"<b>Required minimum:</b> {min_viable:,.2f} DUSK\n\n"
        f"<b>Action:</b> deposit additional DUSK to enable heal."
    )
    send_async(msg, alert_key="insufficient_operator_stake")


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
