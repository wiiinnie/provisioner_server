"""
test_rotation.py — Standalone rotation logic test harness.

Monkey-patches all external calls in lib.rotation before import,
then drives on_block() with synthetic block heights to simulate
epoch transitions in seconds.

Usage:
    cd /root/provisioner_server
    python3 -m test_rotation              # all scenarios (symlink: provisioner_server.py -> server.1.0.6.py)
    python3 test_rotation.py normal       # specific scenario
    python3 test_rotation.py a2
    python3 test_rotation.py step3_fail
    python3 test_rotation.py precheck_skip
    python3 test_rotation.py seed_inactive
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import threading
import time
import types
import unittest
from collections import deque


# ── ANSI colours ──────────────────────────────────────────────────────────────
GRN  = "\033[32m"
RED  = "\033[31m"
YEL  = "\033[33m"
CYN  = "\033[36m"
DIM  = "\033[2m"
RST  = "\033[0m"

def _ok(msg):   print(f"  {GRN}✓{RST} {msg}"); return True
def _fail(msg): print(f"  {RED}✗{RST} {msg}"); return False
def _info(msg): print(f"  {DIM}  {msg}{RST}")


# ── Mock infrastructure ────────────────────────────────────────────────────────

class CmdLog:
    """Records every _cmd() call and returns configurable results."""
    def __init__(self):
        self.calls: list[str] = []
        self._fail_on: set[str] = set()   # keywords → return ok=False
        self._slow:    set[str] = set()   # keywords → add 0.05s delay

    def reset(self):
        self.calls.clear()
        self._fail_on.clear()
        self._slow.clear()

    def fail_on(self, *keywords):
        self._fail_on.update(keywords)

    def __call__(self, cmd_str: str, timeout: int = 90) -> dict:
        self.calls.append(cmd_str)
        for kw in self._fail_on:
            if kw in cmd_str:
                return {"ok": False, "stdout": "", "stderr": f"Mock failure: {kw} rejected"}
        if self._slow:
            for kw in self._slow:
                if kw in cmd_str:
                    time.sleep(0.05)
        return {"ok": True, "stdout": "Hash: aabbccdd1122", "stderr": ""}


class AssessMock:
    """Returns configurable provisioner state."""
    def __init__(self):
        self._state = self._healthy_a1m1()

    def set(self, state: dict):
        self._state = state

    def __call__(self) -> dict:
        return dict(self._state)

    @staticmethod
    def _node(idx, status, ta, stake=4000.0, locked=0.0, reward=0.05,
              addr=None):
        return {
            "idx": idx,
            "address": addr or f"prov{idx}addr{'x'*30}",
            "status": status,
            "ta": ta,
            "stake_dusk": stake,
            "locked_dusk": locked,
            "reward_dusk": reward,
            "has_stake": stake >= 1000.0,
        }

    def _healthy_a1m1(self):
        """A:1 M:1 ta=1 — standard healthy state ready for rotation."""
        n1 = self._node(1, "active",   0, stake=4000.0, locked=320.0, reward=0.05)
        n2 = self._node(2, "maturing", 1, stake=1000.0, locked=0.0,   reward=0.01)
        return {
            "active":   [n1],
            "maturing": [n2],
            "inactive": [],
            "by_idx":   {1: n1, 2: n2},
            "label":    "A:1 M:1 I:0",
            "epoch":    1430,
            "remaining_blocks": 41,
        }

    def healthy_a1m1(self):
        self._state = self._healthy_a1m1()

    def both_active_a2(self, larger_stake=4000.0, smaller_stake=1000.0):
        """A:2 — both rot nodes active, abnormal state."""
        n1 = self._node(1, "active", 0, stake=larger_stake,  reward=0.05)
        n2 = self._node(2, "active", 0, stake=smaller_stake, reward=0.01)
        self._state = {
            "active":   [n1, n2],
            "maturing": [],
            "inactive": [],
            "by_idx":   {1: n1, 2: n2},
            "label":    "A:2 M:0 I:0",
            "epoch":    1430,
            "remaining_blocks": 41,
        }

    def a1_inactive(self):
        """A:1 M:0 I:1 — need to seed inactive."""
        n1 = self._node(1, "active",   0, stake=4000.0)
        n2 = self._node(2, "inactive", None, stake=0.0)
        self._state = {
            "active":   [n1],
            "maturing": [],
            "inactive": [n2],
            "by_idx":   {1: n1, 2: n2},
            "label":    "A:1 M:0 I:1",
            "epoch":    1430,
            "remaining_blocks": 500,
        }

    def a1m1_ta2_not_ready(self):
        """A:1 M:1 ta=2 — rot_slave not ready for rotation this epoch."""
        n1 = self._node(1, "active",  0, stake=4000.0)
        n2 = self._node(2, "seeded",  2, stake=1000.0)
        self._state = {
            "active":   [n1],
            "maturing": [n2],
            "inactive": [],
            "by_idx":   {1: n1, 2: n2},
            "label":    "A:1 M:1 I:0",
            "epoch":    1430,
            "remaining_blocks": 41,
        }

    def all_inactive(self):
        """A:0 M:0 I:2 — bootstrap state."""
        n1 = self._node(1, "inactive", None, stake=0.0)
        n2 = self._node(2, "inactive", None, stake=0.0)
        self._state = {
            "active":   [],
            "maturing": [],
            "inactive": [n1, n2],
            "by_idx":   {1: n1, 2: n2},
            "label":    "A:0 M:0 I:2",
            "epoch":    1430,
            "remaining_blocks": 500,
        }


# ── Patch and import rotation ──────────────────────────────────────────────────

def _build_fake_lib():
    """Build a minimal fake lib package so rotation.py can be imported."""
    import os as _os
    _lib_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "lib")
    lib = types.ModuleType("lib")
    lib.__path__ = [_lib_dir]
    lib.__package__ = "lib"

    # config
    cfg_mod = types.ModuleType("lib.config")
    cfg_mod._log               = lambda msg: None   # suppress noise
    cfg_mod._ROTATION_LOG_PATH = "/tmp/test_rotation.log"
    cfg_mod.cfg                = lambda key: {
        "master_idx":          0,
        "rotation_window":     41,
        "snatch_window":       11,
        "min_deposit_dusk":    100.0,
        "gas_limit":           3000000,
        "gas_price":           2,
        "prov_1_address":      "prov1addr" + "x" * 30,
        "prov_2_address":      "prov2addr" + "x" * 30,
    }.get(key)
    cfg_mod.NODE_INDICES       = [0, 1, 2]
    cfg_mod._save_rotation_phase  = lambda p: None
    cfg_mod._load_rotation_phase  = lambda: "rotating"
    cfg_mod._save_rotation_enabled = lambda e: None
    cfg_mod._load_rotation_enabled = lambda: False
    cfg_mod._ROTATION_LOG_PATH = "/tmp/test_rotation.log"

    # wallet
    wallet_mod = types.ModuleType("lib.wallet")
    wallet_mod.WALLET_PATH = "/tmp/fake_wallet"
    wallet_mod.get_password = lambda: "testpassword"

    # rues — register_tx_confirm returns an Event that fires immediately
    rues_mod = types.ModuleType("lib.rues")
    def _instant_confirm(fn_name):
        evt = threading.Event()
        evt.set()   # pre-fire — confirms instantly
        return evt
    rues_mod.register_tx_confirm = _instant_confirm

    # assess — will be replaced per-test
    assess_mod = types.ModuleType("lib.assess")
    assess_mod._assess_state         = lambda tip, pw: {}
    assess_mod._assess_state_cached  = lambda tip, pw, force=False: {}
    assess_mod._fetch_capacity_cached = lambda pw, force=False: {
        "active_current": 427000.0, "active_maximum": 3000000.0,
        "locked_current": 320.0,    "locked_maximum": 60000.0,
    }
    assess_mod._invalidate_all_caches = lambda: None

    # pool
    pool_mod = types.ModuleType("lib.pool")
    pool_mod._pool_fetch_real = lambda pw="": 5000.0
    pool_mod._fast_alloc_pool = lambda: 5000.0
    pool_mod._pool_delta      = lambda d: None

    # events
    events_mod = types.ModuleType("lib.events")
    events_mod.mark_sweeper_activation = lambda addr: None

    sys.modules["lib"]           = lib
    sys.modules["lib.config"]    = cfg_mod
    sys.modules["lib.wallet"]    = wallet_mod
    sys.modules["lib.rues"]      = rues_mod
    sys.modules["lib.assess"]    = assess_mod
    sys.modules["lib.pool"]      = pool_mod
    sys.modules["lib.events"]    = events_mod
    return cfg_mod, wallet_mod, rues_mod, assess_mod, pool_mod


cfg_mod, wallet_mod, rues_mod, assess_mod, pool_mod = _build_fake_lib()

# Now import rotation — it will use our mocked modules
import importlib, sys as _sys
# Ensure it loads fresh
for key in list(_sys.modules.keys()):
    if "rotation" in key:
        del _sys.modules[key]

import lib.rotation as rot


# ── Per-test plumbing ──────────────────────────────────────────────────────────

cmd_log  = CmdLog()
assess_m = AssessMock()

def _patch_rotation():
    """Wire our mocks into the already-imported rotation module."""
    rot._cmd     = cmd_log
    rot._assess  = assess_m
    rot._pool_dusk = lambda: pool_mod._pool_fetch_real()
    rot._pw      = lambda: "testpassword"

def _reset(enable=True):
    """Reset rotation state between tests."""
    cmd_log.reset()
    with rot._state_lock:
        rot._rotation_state.update({
            "enabled":                enable,
            "state":                  rot.ROTATING if enable else rot.IDLE,
            "status_msg":             "",
            "last_rotation_epoch":    -1,
            "last_state_check_block": -1,
            "precheck_done_epoch":    -1,
            "pending_terminate":      None,
            "pending_deactivate":     None,
            "waiting_for_ta1_idx":    None,
            "sweeper_candidate_block": -1,
            "sweeper_candidate_dusk":  0.0,
        })
    with rot._sweep_delta_lock:
        rot._sweep_delta = 0.0
    rot._save_state = lambda: None   # don't write ~/.sozu_rotation.json during tests

def _drive_to_rotation_window(near_block=3_086_400):
    """
    Feed on_block() with heights that:
      1. Trigger a state check (blk_left=500, well in regular window)
      2. Trigger pre-check (blk_left = rot_win + 5 = 46)
      3. Enter rotation window (blk_left = rot_win - 1 = 40)
    Uses epoch-aligned base so blk_left values are exact.
    Returns the block heights used.
    """
    epoch_size = rot.EPOCH_BLOCKS  # 2160

    # Align to nearest epoch boundary so blk_left math is exact
    epoch_base = (near_block // epoch_size) * epoch_size

    b_state = epoch_base + epoch_size - 500   # blk_left = 500
    b_pre   = epoch_base + epoch_size - 46    # blk_left = 46 (pre-check)
    b_rot   = epoch_base + epoch_size - 40    # blk_left = 40 (rotation window)

    for b in (b_state, b_pre):
        rot.on_block(b)
        time.sleep(0.2)

    rot.on_block(b_rot)
    time.sleep(1.5)   # rotation sequence runs in background thread — give it time

    return b_state, b_pre, b_rot

def _cmds_contain(keyword):
    return any(keyword in c for c in cmd_log.calls)

def _cmd_count(keyword):
    return sum(1 for c in cmd_log.calls if keyword in c)


# ── Test scenarios ─────────────────────────────────────────────────────────────

def scenario_normal_rotation():
    print(f"\n{CYN}▶ Scenario 1: Normal healthy rotation (A:1 M:1 ta=1){RST}")
    _reset()
    assess_m.healthy_a1m1()
    _patch_rotation()

    _drive_to_rotation_window()

    # Verify sequence
    liq_ok   = _cmds_contain("liquidate")
    term_ok  = _cmds_contain("terminate")
    alloc_ok = _cmd_count("stake-activate") >= 1   # bulk alloc
    seed_ok  = _cmd_count("stake-activate") >= 2   # re-seed

    liq_ok  and _ok("liquidate fired")       or _fail("liquidate NOT fired")
    term_ok and _ok("terminate fired")       or _fail("terminate NOT fired")
    alloc_ok and _ok("bulk allocation fired") or _fail("bulk allocation NOT fired")
    seed_ok and _ok("re-seed fired")         or _fail("re-seed NOT fired")

    # Verify order: alloc before seed
    activate_calls = [c for c in cmd_log.calls if "stake-activate" in c]
    if len(activate_calls) >= 2:
        # alloc goes to rot_slave (prov2), seed goes to rot_active (prov1)
        alloc_first = "prov2" in activate_calls[0] or "prov1" not in activate_calls[0]
        alloc_first and _ok("step order correct: allocate → re-seed") \
                    or _fail("step order WRONG — re-seed before allocate")
    else:
        _fail(f"expected 2 stake-activate calls, got {len(activate_calls)}")

    # Verify alloc amount: stake(4000) + locked(320) + reward(0.05) - seed(1000) = 3320.05
    alloc_call = next((c for c in cmd_log.calls if "stake-activate" in c
                       and "prov2" in c), "")
    if alloc_call:
        import re
        m = re.search(r"--amount (\d+)", alloc_call)
        if m:
            alloc_lux  = int(m.group(1))
            alloc_dusk = alloc_lux / 1e9
            expected   = 4000.0 + 320.0 + 0.05 - 1000.0   # 3320.05
            close      = abs(alloc_dusk - expected) < 0.01
            close and _ok(f"allocation amount correct: {alloc_dusk:.4f} DUSK") \
                  or _fail(f"allocation amount wrong: {alloc_dusk:.4f} (expected ~{expected:.4f})")

    state = rot._rotation_state["state"]
    state == rot.ROTATING and _ok(f"final state: {state}") \
                           or _fail(f"final state: {state} (expected rotating)")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a2_recovery():
    print(f"\n{CYN}▶ Scenario 2: A:2 recovery — both rot nodes active{RST}")
    _reset()
    assess_m.both_active_a2(larger_stake=4000.0, smaller_stake=1000.0)
    _patch_rotation()

    _drive_to_rotation_window()

    liq_ok  = _cmds_contain("liquidate")
    term_ok = _cmds_contain("terminate")
    seed_ok = _cmds_contain("stake-activate")

    liq_ok  and _ok("liquidate fired on smaller node")  or _fail("liquidate NOT fired")
    term_ok and _ok("terminate fired")                  or _fail("terminate NOT fired")
    seed_ok and _ok("re-seed fired")                    or _fail("re-seed NOT fired")

    # Verify no bulk allocation — no ta=1 target exists
    activate_calls = [c for c in cmd_log.calls if "stake-activate" in c]
    len(activate_calls) == 1 \
        and _ok("no bulk allocation (correct — no ta=1 target)") \
        or  _fail(f"unexpected {len(activate_calls)} stake-activate calls (expected 1)")

    # Verify correct target: prov2 (smaller, 1000 DUSK)
    liq_call = next((c for c in cmd_log.calls if "liquidate" in c), "")
    "prov2" in liq_call \
        and _ok("liquidated correct node (prov2, smaller stake)") \
        or  _fail(f"wrong liquidation target in: {liq_call[:60]}")

    state = rot._rotation_state["state"]
    state == rot.ROTATING and _ok(f"final state: {state}") \
                           or _fail(f"final state: {state} (expected rotating)")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_step3_alloc_fail():
    print(f"\n{CYN}▶ Scenario 3: Step 3 allocation fails — rotation still completes{RST}")
    _reset()
    assess_m.healthy_a1m1()
    _patch_rotation()
    cmd_log.fail_on("stake-activate")   # both activate calls fail

    _drive_to_rotation_window()

    liq_ok  = _cmds_contain("liquidate")
    term_ok = _cmds_contain("terminate")
    liq_ok  and _ok("liquidate fired")  or _fail("liquidate NOT fired")
    term_ok and _ok("terminate fired")  or _fail("terminate NOT fired")

    # Both stake-activate fired (alloc + seed) even though both failed
    n_activate = _cmd_count("stake-activate")
    n_activate >= 1 and _ok(f"stake-activate attempted ({n_activate}x despite failure)") \
                    or  _fail("stake-activate never attempted")

    # State should NOT be ERROR — allocation failure is non-fatal
    state = rot._rotation_state["state"]
    state != rot.ERROR \
        and _ok(f"state not ERROR after alloc failure: {state}") \
        or  _fail(f"state is ERROR — allocation failure should be non-fatal")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_precheck_skip_ta2():
    print(f"\n{CYN}▶ Scenario 4: Pre-check skips when rot_slave is ta=2 (not ready){RST}")
    _reset()
    assess_m.a1m1_ta2_not_ready()
    _patch_rotation()

    _drive_to_rotation_window()

    # No liquidate/terminate should fire
    liq_fired = _cmds_contain("liquidate")
    not liq_fired and _ok("liquidate NOT fired (correct — ta=2 not ready)") \
                  or  _fail("liquidate fired unexpectedly for ta=2 slave")

    # last_rotation_epoch should be bumped (skipped, not attempted)
    bumped = rot._rotation_state["last_rotation_epoch"] != -1
    bumped and _ok("epoch bumped after skip") \
           or  _fail("epoch NOT bumped — may re-attempt next block")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_seed_inactive():
    print(f"\n{CYN}▶ Scenario 5: State check seeds inactive node (A:1 M:0 I:1){RST}")
    _reset()
    assess_m.a1_inactive()
    _patch_rotation()

    # Just drive a state check block (well outside rotation window)
    epoch_start = 3_086_400
    b_state = epoch_start + rot.EPOCH_BLOCKS - 500
    rot.on_block(b_state)
    time.sleep(0.2)

    seed_ok = _cmds_contain("stake-activate")
    seed_ok and _ok("seed fired for inactive node") \
            or  _fail("seed NOT fired for inactive node")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_bootstrap_all_inactive():
    print(f"\n{CYN}▶ Scenario 6: Bootstrap — A:0 M:0 I:2, seed first node{RST}")
    _reset()
    assess_m.all_inactive()
    _patch_rotation()

    epoch_start = 3_086_400
    b_state = epoch_start + rot.EPOCH_BLOCKS - 500
    rot.on_block(b_state)
    time.sleep(0.2)

    seed_ok = _cmds_contain("stake-activate")
    seed_ok and _ok("first seed fired") \
            or  _fail("first seed NOT fired")

    waiting = rot._rotation_state.get("waiting_for_ta1_idx")
    waiting is not None \
        and _ok(f"waiting_for_ta1_idx set → prov[{waiting}]") \
        or  _fail("waiting_for_ta1_idx not set after first seed")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_liquidate_fails():
    print(f"\n{CYN}▶ Scenario 7: Liquidate fails — rotation aborts cleanly{RST}")
    _reset()
    assess_m.healthy_a1m1()
    _patch_rotation()
    cmd_log.fail_on("liquidate")

    _drive_to_rotation_window()

    # terminate and stake-activate must NOT fire after liquidate failure
    term_fired     = _cmds_contain("terminate")
    activate_fired = _cmds_contain("stake-activate")

    not term_fired     and _ok("terminate not fired after liquidate failure") \
                       or  _fail("terminate fired after liquidate failure")
    not activate_fired and _ok("stake-activate not fired after liquidate failure") \
                       or  _fail("stake-activate fired after liquidate failure")

    state = rot._rotation_state["state"]
    state != rot.ERROR \
        and _ok(f"state after abort: {state} (bumped epoch, stays rotating)") \
        or  _fail(f"state is ERROR — liquidate abort should not be fatal")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


# ── Runner ─────────────────────────────────────────────────────────────────────

# ── Capacity mock helper ──────────────────────────────────────────────────────

_cap_mock = {
    "active_current": 427000.0, "active_maximum": 3000000.0,
    "locked_current": 0.0,      "locked_maximum": 60000.0,
}

def _set_capacity(active_current, active_maximum=3000000.0, locked_current=0.0):
    global _cap_mock
    _cap_mock = {
        "active_current": active_current, "active_maximum": active_maximum,
        "locked_current": locked_current, "locked_maximum": 60000.0,
    }
    assess_mod._fetch_capacity_cached = lambda pw="", force=False: dict(_cap_mock)
    assess_mod._fetch_capacity        = lambda pw="": dict(_cap_mock)
    assess_mod._invalidate_capacity_cache = lambda: None

def _reset_capacity():
    _set_capacity(427000.0)


# ── New assess states ─────────────────────────────────────────────────────────

def _a1m1_at_max(am):
    n1 = am._node(1, "active",   0, stake=2848531.0, locked=1197.0, reward=116.0)
    n2 = am._node(2, "maturing", 1, stake=1000.0,    locked=0.0,    reward=0.0)
    am._state = {
        "active": [n1], "maturing": [n2], "inactive": [],
        "by_idx": {1: n1, 2: n2}, "label": "A:1 M:1 I:0",
        "epoch": 1430, "remaining_blocks": 41,
    }

def _a1_inactive_at_max(am):
    n1 = am._node(1, "active",   0, stake=2998999.0, locked=0.0, reward=108.0)
    n2 = am._node(2, "inactive", None, stake=0.0)
    am._state = {
        "active": [n1], "maturing": [], "inactive": [n2],
        "by_idx": {1: n1, 2: n2}, "label": "A:1 M:0 I:1",
        "epoch": 1430, "remaining_blocks": 500,
    }


# ── New scenarios ─────────────────────────────────────────────────────────────

def scenario_rotation_at_max_capacity():
    print(f"\n{CYN}▶ Scenario 8: Rotation at max capacity — step 3 not capped{RST}")
    _reset()
    _a1m1_at_max(assess_m)
    _set_capacity(active_current=3000000.0)
    _patch_rotation()

    _drive_to_rotation_window()

    liq_ok = _cmds_contain("liquidate")
    liq_ok and _ok("liquidate fired") or _fail("liquidate NOT fired")
    _cmds_contain("terminate") and _ok("terminate fired") or _fail("terminate NOT fired")

    activate_calls = [c for c in cmd_log.calls if "stake-activate" in c]
    len(activate_calls) >= 1 \
        and _ok(f"stake-activate fired ({len(activate_calls)}x) — capacity did not block rotation") \
        or  _fail("stake-activate NOT fired — capacity wrongly blocked rotation")

    import re
    alloc_call = next((c for c in cmd_log.calls if "stake-activate" in c and "prov2" in c), "")
    if alloc_call:
        m = re.search(r"--amount (\d+)", alloc_call)
        if m:
            alloc_dusk = int(m.group(1)) / 1e9
            expected   = 2848531.0 + 1197.0 + 116.0 - 1000.0
            abs(alloc_dusk - expected) < 1.0 \
                and _ok(f"alloc amount correct: {alloc_dusk:,.2f} DUSK") \
                or  _fail(f"alloc amount wrong: {alloc_dusk:,.2f} (expected {expected:,.2f})")

    state = rot._rotation_state["state"]
    state == rot.ROTATING and _ok(f"final state: {state}") or _fail(f"wrong state: {state}")
    _reset_capacity()
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_sweeper_skips_at_max():
    print(f"\n{CYN}▶ Scenario 9: Sweeper skips when capacity full{RST}")
    _reset()
    assess_m.healthy_a1m1()
    _set_capacity(active_current=3000000.0)
    pool_mod._pool_fetch_real = lambda pw="": 5000.0
    _patch_rotation()

    epoch_base = (3_086_400 // rot.EPOCH_BLOCKS) * rot.EPOCH_BLOCKS
    rot.on_block(epoch_base + rot.EPOCH_BLOCKS - 500)
    time.sleep(0.4)
    rot.on_block(epoch_base + rot.EPOCH_BLOCKS - 490)
    time.sleep(0.4)

    not _cmds_contain("stake-activate") \
        and _ok("sweeper correctly skipped — capacity full") \
        or  _fail("sweeper fired despite capacity full")

    _reset_capacity()
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_seed_skips_at_max():
    print(f"\n{CYN}▶ Scenario 10: State check skips seeding when capacity full{RST}")
    _reset()
    _a1_inactive_at_max(assess_m)
    _set_capacity(active_current=3000000.0)
    pool_mod._pool_fetch_real = lambda pw="": 500.0
    _patch_rotation()

    epoch_base = (3_086_400 // rot.EPOCH_BLOCKS) * rot.EPOCH_BLOCKS
    rot.on_block(epoch_base + rot.EPOCH_BLOCKS - 500)
    time.sleep(0.5)

    not _cmds_contain("stake-activate") \
        and _ok("seeding correctly skipped — capacity full") \
        or  _fail("seeding fired despite capacity full")

    _reset_capacity()
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


# ── Scenario registry ─────────────────────────────────────────────────────────

SCENARIOS = {
    "normal":           scenario_normal_rotation,
    "a2":               scenario_a2_recovery,
    "step3_fail":       scenario_step3_alloc_fail,
    "precheck_skip":    scenario_precheck_skip_ta2,
    "seed_inactive":    scenario_seed_inactive,
    "bootstrap":        scenario_bootstrap_all_inactive,
    "liq_fail":         scenario_liquidate_fails,
    "rotation_max_cap": scenario_rotation_at_max_capacity,
    "sweeper_max_cap":  scenario_sweeper_skips_at_max,
    "seed_skip":        scenario_seed_skips_at_max,
}

if __name__ == "__main__":
    _patch_rotation()

    requested = sys.argv[1:] if len(sys.argv) > 1 else list(SCENARIOS.keys())
    unknown   = [s for s in requested if s not in SCENARIOS]
    if unknown:
        print(f"Unknown scenarios: {unknown}")
        print(f"Available: {list(SCENARIOS.keys())}")
        sys.exit(1)

    print(f"\n{CYN}{'='*60}")
    print(f"  SOZU Rotation Automation — Test Harness")
    print(f"  Running {len(requested)} scenario(s)")
    print(f"{'='*60}{RST}")

    for name in requested:
        SCENARIOS[name]()

    print(f"\n{CYN}{'='*60}{RST}\n")
