#!/usr/bin/env python3
"""
apply_heal_hooks.py — Idempotently patch lib/rotation.py to add v2 heal hooks.

Run from the repo root:
    python apply_heal_hooks.py

Idempotent: safe to re-run after pulling new rotation.py changes.
Creates a .bak-heal-YYYYMMDD_HHMMSS backup before writing.

v2 adds 4 hooks to rotation.py (via 5 patch entries — hook #3 is in two places):

  1. on_block()                — tick_heal() at start (handles epoch-boundary role swap)
  2. _run_state_check()        — check_threshold_and_trigger in master-threshold branch
  3. _run_rotation() START     — run_harvest() takeover when heal is in SEEDED state
     _run_rotation() END       — perform_epoch_n_seed() append after step 4
     _run_rotation() STEP 3    — reserve extra SEED_DUSK for heal's epoch-N seed
"""

import os
import sys
import shutil
from datetime import datetime


ROT_PATH = "lib/rotation.py"


PATCHES = [
    # ── 1. on_block: add tick_heal at start ───────────────────────────────
    {
        "name":     "on_block tick_heal",
        "sentinel": "from .heal import tick_heal",
        "find": '''def on_block(block_height: int) -> None:
    global _sweep_delta
    _log(f"[rotation] on_block called height={block_height}")
    with _state_lock:''',
        "replace": '''def on_block(block_height: int) -> None:
    global _sweep_delta
    _log(f"[rotation] on_block called height={block_height}")

    # ── HEAL hook: per-block heal tick (role swap on epoch boundary) ──────────
    try:
        from .heal import tick_heal
        tick_heal(block_height)
    except Exception as _heal_err:
        _log(f"[rotation] heal tick error: {_heal_err}")

    with _state_lock:''',
    },

    # ── 2. _run_state_check: replace single-threshold with two-threshold + heal ──
    # v2: alert and heal use separate percentages, both applied to target_master
    # (= active_max − rotation_floor). This patch rewrites the threshold block
    # from the old single-threshold-vs-max_cap logic to the new two-threshold model.
    {
        "name":     "state_check two-threshold + heal trigger",
        "sentinel": "master_alert_threshold_pct",
        "find": '''                threshold_pct = float(cfg("master_threshold_pct") or 15.0)
                master_node   = nodes.get(master_idx, {})
                master_stake  = master_node.get("stake_dusk", 0.0)
                cap           = _fcc(_pw())
                active_max    = cap.get("active_maximum", 0.0)
                threshold_dusk = active_max * threshold_pct / 100.0
                # Compare in LUX to avoid float issues
                master_lux    = round(master_stake    * 1e9)
                threshold_lux = round(threshold_dusk  * 1e9)
                if master_lux < threshold_lux:
                    _rlog_warn(f"master prov[{master_idx}] stake {master_stake:,.2f} DUSK "
                               f"< threshold {threshold_dusk:,.2f} DUSK "
                               f"({threshold_pct:.0f}% of {active_max:,.0f}) — sending alert")
                    try:
                        from .telegram import alert_master_below_threshold
                        alert_master_below_threshold(
                            master_idx, master_stake, threshold_dusk,
                            threshold_pct, active_max)
                    except Exception as _tg_err:
                        _rlog_warn(f"telegram alert error: {_tg_err}")
                else:''',
        "replace": '''                # v2 heal: two-threshold model (alert + heal), both applied to
                # target_master. Backward-compat: fall back to old master_threshold_pct.
                cap           = _fcc(_pw())
                active_max    = cap.get("active_maximum", 0.0)
                rot_floor_pct = float(cfg("rotation_floor_pct") or 20.0)
                rotation_floor = active_max * rot_floor_pct / 100.0
                target_master = max(0.0, active_max - rotation_floor)

                alert_pct = float(cfg("master_alert_threshold_pct")
                                  or cfg("master_threshold_pct")
                                  or 70.0)
                heal_pct  = float(cfg("master_heal_threshold_pct")
                                  or cfg("master_threshold_pct")
                                  or 50.0)

                master_node   = nodes.get(master_idx, {})
                master_stake  = master_node.get("stake_dusk", 0.0)

                alert_dusk = target_master * alert_pct / 100.0
                heal_dusk  = target_master * heal_pct  / 100.0

                master_lux = round(master_stake * 1e9)
                alert_lux  = round(alert_dusk   * 1e9)

                if master_lux < alert_lux:
                    _rlog_warn(f"master prov[{master_idx}] stake {master_stake:,.2f} DUSK "
                               f"< alert threshold {alert_dusk:,.2f} DUSK "
                               f"({alert_pct:.0f}% of target_master={target_master:,.0f}) — "
                               f"sending alert")
                    try:
                        from .telegram import alert_master_below_threshold
                        alert_master_below_threshold(
                            master_idx, master_stake, alert_dusk,
                            alert_pct, target_master)
                    except Exception as _tg_err:
                        _rlog_warn(f"telegram alert error: {_tg_err}")
                    # ── HEAL hook: notify heal; heal uses its own (lower) threshold ──
                    try:
                        from .heal import check_threshold_and_trigger
                        from .pool import _fast_alloc_pool
                        check_threshold_and_trigger(
                            master_idx, master_stake, active_max, _fast_alloc_pool())
                    except Exception as _heal_err:
                        _rlog_warn(f"heal trigger error: {_heal_err}")
                else:''',
    },

    # ── 3a. _run_rotation START: heal harvest takeover ─────────────────────
    {
        "name":     "run_rotation heal harvest takeover",
        "sentinel": "from .heal import should_run_harvest, run_harvest",
        "find": '''def _run_rotation(cur_epoch: int) -> None:
    _set_state(IN_PROGRESS)
    try:
        from .wallet import WALLET_PATH
        from .rues import register_tx_confirm, get_tx_confirm_result''',
        "replace": '''def _run_rotation(cur_epoch: int) -> None:
    _set_state(IN_PROGRESS)
    try:
        # ── HEAL hook: in epoch N+1, heal takes the rotation window ─────
        try:
            from .heal import should_run_harvest, run_harvest
            if should_run_harvest(cur_epoch):
                _rlog_step(f"heal: running HARVEST in place of regular rotation")
                run_harvest(cur_epoch)
                _set_state(ROTATING)
                _bump_epoch(cur_epoch)
                return
        except Exception as _heal_err:
            _rlog_warn(f"heal harvest hook error: {_heal_err}")

        from .wallet import WALLET_PATH
        from .rues import register_tx_confirm, get_tx_confirm_result''',
    },

    # ── 3b. _run_rotation END: epoch-N seed append after step 4 ────────────
    {
        "name":     "run_rotation epoch-N seed append",
        "sentinel": "from .heal import wants_epoch_n_seed, perform_epoch_n_seed",
        "find": '''            _rlog_ok(f"[4/4] prov[{rot_active_idx}] re-seeded {SEED_DUSK:.0f} DUSK → ta=2 (confirmed)")

        _rlog_ok(f"─── rotation epoch {cur_epoch} complete ✓ ───")''',
        "replace": '''            _rlog_ok(f"[4/4] prov[{rot_active_idx}] re-seeded {SEED_DUSK:.0f} DUSK → ta=2 (confirmed)")

        # ── HEAL hook: in epoch N, append one extra seed for standby master ──
        # After rotation's step 4 confirms, if heal is awaiting the epoch-N seed,
        # fire stake_activate on standby (perform_epoch_n_seed handles its own
        # block-gap wait).
        try:
            from .heal import wants_epoch_n_seed, perform_epoch_n_seed
            if wants_epoch_n_seed():
                _rlog_step("heal: appending epoch-N seed of standby after rotation")
                perform_epoch_n_seed(cur_epoch)
        except Exception as _heal_err:
            _rlog_warn(f"heal epoch-N seed hook error: {_heal_err}")

        _rlog_ok(f"─── rotation epoch {cur_epoch} complete ✓ ───")''',
    },

    # ── 4. _run_rotation STEP 3: reserve extra SEED_DUSK for heal's epoch-N seed ──
    # Without this, rotation consumes all freed stake into rot_slave + re-seed,
    # leaving nothing for heal's append. Heal would fail with "insufficient pool
    # balance" on its stake_activate. The reserve ensures 1k stays in pool for heal.
    {
        "name":     "run_rotation step 3 heal reserve",
        "sentinel": "heal_reserve = SEED_DUSK",
        "find": '''        alloc_dusk = max(0.0, freed_by_liquidate + freed_by_terminate - SEED_DUSK)
        _rlog_step(f"[3/4] allocate freed stake → rot_slave prov[{rot_slave_idx}] (ta=1, no slash)")
        _rlog_info(
            f"[3/4] liquidate freed {freed_by_liquidate:.4f} DUSK + "
            f"terminate freed {freed_by_terminate:.4f} DUSK - "
            f"seed {SEED_DUSK:.0f} DUSK = {alloc_dusk:.4f} DUSK to allocate")''',
        "replace": '''        # ── HEAL hook: if heal is awaiting its epoch-N standby seed, reserve an
        # extra SEED_DUSK so the heal hook at end of rotation has something to fire.
        heal_reserve = 0.0
        try:
            from .heal import wants_epoch_n_seed
            if wants_epoch_n_seed():
                heal_reserve = SEED_DUSK
        except Exception:
            pass
        alloc_dusk = max(0.0, freed_by_liquidate + freed_by_terminate - SEED_DUSK - heal_reserve)
        _rlog_step(f"[3/4] allocate freed stake → rot_slave prov[{rot_slave_idx}] (ta=1, no slash)")
        _rlog_info(
            f"[3/4] liquidate freed {freed_by_liquidate:.4f} DUSK + "
            f"terminate freed {freed_by_terminate:.4f} DUSK - "
            f"seed {SEED_DUSK:.0f} DUSK"
            + (f" - heal_reserve {heal_reserve:.0f} DUSK" if heal_reserve > 0 else "")
            + f" = {alloc_dusk:.4f} DUSK to allocate")''',
    },
]


def apply_patches():
    if not os.path.exists(ROT_PATH):
        print(f"✖ {ROT_PATH} not found. Run from the repo root.", file=sys.stderr)
        sys.exit(1)

    with open(ROT_PATH) as f:
        content = f.read()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{ROT_PATH}.bak-heal-{ts}"
    shutil.copy(ROT_PATH, backup)
    print(f"  backup: {backup}")

    changes = 0
    skipped = 0
    failed  = []

    for p in PATCHES:
        name = p["name"]
        if p["sentinel"] in content:
            print(f"  [skip] {name} (already applied)")
            skipped += 1
            continue
        if p["find"] not in content:
            print(f"  [FAIL] {name}: anchor text not found — rotation.py may have diverged")
            failed.append(name)
            continue
        content = content.replace(p["find"], p["replace"], 1)
        print(f"  [ok]   {name}")
        changes += 1

    if failed:
        print(f"\n✖ {len(failed)} patch(es) failed to apply: {', '.join(failed)}")
        print(f"  rotation.py was NOT modified. Manual intervention needed.")
        sys.exit(1)

    if changes == 0:
        print(f"\n  All hooks already applied — nothing to do.")
        os.remove(backup)
        return

    with open(ROT_PATH, "w") as f:
        f.write(content)
    print(f"\n✓ Applied {changes} hooks, skipped {skipped} already-applied. "
          f"rotation.py updated.")
    print(f"  To revert: mv {backup} {ROT_PATH}")


if __name__ == "__main__":
    apply_patches()
