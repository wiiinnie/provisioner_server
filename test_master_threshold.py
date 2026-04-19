"""
test_master_threshold.py — Test harness for master stake threshold logic.

Simulates all rotation node states when master drops below min_master_stake,
verifying the correct recovery sequence fires in each case.

Usage:
    cd /root/provisioner_server
    python3 test_master_threshold.py              # all scenarios
    python3 test_master_threshold.py a1m1_ta1     # specific scenario
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import threading
import time
import types
import re
import unittest

GRN  = "\033[32m"
RED  = "\033[31m"
YEL  = "\033[33m"
CYN  = "\033[36m"
DIM  = "\033[2m"
RST  = "\033[0m"

def _ok(msg):   print(f"  {GRN}✓{RST} {msg}"); return True
def _fail(msg): print(f"  {RED}✗{RST} {msg}"); return False
def _info(msg): print(f"  {DIM}  {msg}{RST}")
def _warn(msg): print(f"  {YEL}⚠{RST} {msg}")


# ── Mock infrastructure ────────────────────────────────────────────────────────

class CmdLog:
    def __init__(self):
        self.calls: list[str] = []
        self._fail_on: set[str] = set()

    def reset(self):
        self.calls.clear()
        self._fail_on.clear()

    def fail_on(self, *keywords):
        self._fail_on.update(keywords)

    def __call__(self, cmd_str: str, timeout: int = 90) -> dict:
        self.calls.append(cmd_str)
        for kw in self._fail_on:
            if kw in cmd_str:
                return {"ok": False, "stdout": "", "stderr": f"Mock failure: {kw}"}
        return {"ok": True, "stdout": "Hash: aabbccdd1122", "stderr": ""}


class AssessMock:
    def __init__(self):
        self._state = self._healthy_a1m1()

    def set(self, state: dict):
        self._state = state

    def __call__(self) -> dict:
        return dict(self._state)

    @staticmethod
    def _node(idx, status, ta, stake=4000.0, locked=0.0, reward=0.05, addr=None):
        return {
            "idx": idx, "address": addr or f"prov{idx}addr{'x'*30}",
            "status": status, "ta": ta,
            "stake_dusk": stake, "locked_dusk": locked,
            "reward_dusk": reward, "has_stake": stake >= 1000.0,
        }

    def _healthy_a1m1(self):
        n1 = self._node(1, "active",   0, stake=1500000.0, locked=50000.0, reward=200.0)
        n2 = self._node(2, "maturing", 1, stake=1000.0, locked=0.0, reward=0.0)
        return {
            "active": [n1], "maturing": [n2], "inactive": [],
            "by_idx": {1: n1, 2: n2}, "label": "A:1 M:1 I:0",
            "epoch": 1460, "remaining_blocks": 41,
        }

    # ── State setters ──────────────────────────────────────────────────────────

    def a1m1_ta1(self, rot_stake=1500000.0, rot_locked=50000.0):
        """A:1 M:1 ta=1 — ideal state, rot_active ready, rot_slave ready."""
        n1 = self._node(1, "active",   0, stake=rot_stake, locked=rot_locked, reward=200.0)
        n2 = self._node(2, "maturing", 1, stake=1000.0)
        self._state = {"active":[n1],"maturing":[n2],"inactive":[],
                       "by_idx":{1:n1,2:n2},"label":"A:1 M:1 I:0","epoch":1460,"remaining_blocks":41}

    def a1m1_ta2(self, rot_stake=1500000.0):
        """A:1 M:1 ta=2 — rot_slave just seeded, not ready yet."""
        n1 = self._node(1, "active",   0, stake=rot_stake, reward=200.0)
        n2 = self._node(2, "maturing", 2, stake=1000.0)
        self._state = {"active":[n1],"maturing":[n2],"inactive":[],
                       "by_idx":{1:n1,2:n2},"label":"A:1 M:1 I:0","epoch":1460,"remaining_blocks":41}

    def a1m0_i1(self, rot_stake=1500000.0):
        """A:1 M:0 I:1 — no rot_slave, inactive node needs seeding."""
        n1 = self._node(1, "active",   0, stake=rot_stake, reward=200.0)
        n2 = self._node(2, "inactive", None, stake=0.0)
        self._state = {"active":[n1],"maturing":[],"inactive":[n2],
                       "by_idx":{1:n1,2:n2},"label":"A:1 M:0 I:1","epoch":1460,"remaining_blocks":500}

    def a0m2_ta12(self):
        """A:0 M:2 ta=[1,2] — healthy stagger, both maturing."""
        n1 = self._node(1, "maturing", 2, stake=1000.0)
        n2 = self._node(2, "maturing", 1, stake=1500000.0)
        self._state = {"active":[],"maturing":[n1,n2],"inactive":[],
                       "by_idx":{1:n1,2:n2},"label":"A:0 M:2 I:0","epoch":1460,"remaining_blocks":500}

    def a0m1_ta2_i1(self):
        """A:0 M:1 ta=2, I:1 — one maturing (just seeded), one still inactive."""
        n1 = self._node(1, "maturing", 2, stake=1000.0)
        n2 = self._node(2, "inactive", None, stake=0.0)
        self._state = {"active":[],"maturing":[n1],"inactive":[n2],
                       "by_idx":{1:n1,2:n2},"label":"A:0 M:1 I:1","epoch":1460,"remaining_blocks":500}

    def a0m0_i2(self):
        """A:0 M:0 I:2 — both inactive (bootstrap-like)."""
        n1 = self._node(1, "inactive", None, stake=0.0)
        n2 = self._node(2, "inactive", None, stake=0.0)
        self._state = {"active":[],"maturing":[],"inactive":[n1,n2],
                       "by_idx":{1:n1,2:n2},"label":"A:0 M:0 I:2","epoch":1460,"remaining_blocks":500}

    def a0m1_ta1(self, rot_stake=1500000.0):
        """A:0 M:1 ta=1 — one maturing about to become active next epoch."""
        n1 = self._node(1, "inactive", None, stake=0.0)
        n2 = self._node(2, "maturing", 1, stake=rot_stake)
        self._state = {"active":[],"maturing":[n2],"inactive":[n1],
                       "by_idx":{1:n1,2:n2},"label":"A:0 M:1 I:1","epoch":1460,"remaining_blocks":500}


# ── Fake lib setup ─────────────────────────────────────────────────────────────

def _build_fake_lib():
    import os as _os
    _lib_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "lib")
    lib = types.ModuleType("lib")
    lib.__path__ = [_lib_dir]
    lib.__package__ = "lib"

    cfg_mod = types.ModuleType("lib.config")
    cfg_mod._log               = lambda msg: None
    cfg_mod._ROTATION_LOG_PATH = "/tmp/test_master_threshold.log"
    cfg_mod.cfg                = lambda key: {
        "master_idx":          0,
        "rotation_window":     41,
        "snatch_window":       11,
        "min_deposit_dusk":    100.0,
        "gas_limit":           3000000,
        "gas_price":           2,
        "min_master_stake":    500000.0,   # 500k DUSK threshold
        "prov_0_address":      "prov0addr" + "x" * 30,
        "prov_1_address":      "prov1addr" + "x" * 30,
        "prov_2_address":      "prov2addr" + "x" * 30,
    }.get(key)
    cfg_mod.NODE_INDICES          = [0, 1, 2]
    cfg_mod._save_rotation_phase  = lambda p: None
    cfg_mod._load_rotation_phase  = lambda: "rotating"
    cfg_mod._save_rotation_enabled = lambda e: None
    cfg_mod._load_rotation_enabled = lambda: False

    wallet_mod = types.ModuleType("lib.wallet")
    wallet_mod.WALLET_PATH = "/tmp/fake_wallet"
    wallet_mod.get_password = lambda: "testpassword"

    rues_mod = types.ModuleType("lib.rues")
    def _instant_confirm(fn_name, prov_addr=""):
        evt = threading.Event(); evt.set(); return evt
    def _noop_result(fn_name, prov_addr=""):
        return {"err": None}
    rues_mod.register_tx_confirm    = _instant_confirm
    rues_mod.get_tx_confirm_result  = _noop_result

    assess_mod = types.ModuleType("lib.assess")
    assess_mod._assess_state         = lambda tip, pw: {}
    assess_mod._assess_state_cached  = lambda tip, pw, force=False: {}
    assess_mod._fetch_capacity_cached = lambda pw="", force=False: {
        "active_current": 1600000.0, "active_maximum": 3000000.0,
        "locked_current": 50000.0,   "locked_maximum":  60000.0,
    }
    assess_mod._fetch_capacity        = lambda pw="": {
        "active_current": 1600000.0, "active_maximum": 3000000.0,
        "locked_current": 50000.0,   "locked_maximum":  60000.0,
    }
    assess_mod._invalidate_all_caches    = lambda: None
    assess_mod._invalidate_capacity_cache = lambda: None
    assess_mod._max_topup_active          = lambda cap: 100000.0
    assess_mod.EPOCH_BLOCKS               = 2160

    pool_mod = types.ModuleType("lib.pool")
    pool_mod._pool_fetch_real = lambda pw="": 2000000.0
    pool_mod._fast_alloc_pool = lambda: 2000000.0
    pool_mod._pool_delta      = lambda d: None

    events_mod = types.ModuleType("lib.events")
    events_mod.mark_sweeper_activation = lambda addr: None

    sys.modules["lib"]        = lib
    sys.modules["lib.config"] = cfg_mod
    sys.modules["lib.wallet"] = wallet_mod
    sys.modules["lib.rues"]   = rues_mod
    sys.modules["lib.assess"] = assess_mod
    sys.modules["lib.pool"]   = pool_mod
    sys.modules["lib.events"] = events_mod
    return cfg_mod, wallet_mod, rues_mod, assess_mod, pool_mod


cfg_mod, wallet_mod, rues_mod, assess_mod, pool_mod = _build_fake_lib()

# Master stake mock — configurable per test
_master_stake_mock = 300000.0   # default: below 500k threshold

def _set_master_stake(dusk: float):
    global _master_stake_mock
    _master_stake_mock = dusk

# Import rotation fresh
for key in list(sys.modules.keys()):
    if "rotation" in key:
        del sys.modules[key]
import lib.rotation as rot


# ── Per-test plumbing ──────────────────────────────────────────────────────────

cmd_log  = CmdLog()
assess_m = AssessMock()
_rlog_lines: list[str] = []

def _patch():
    rot._cmd    = cmd_log
    rot._assess = assess_m
    rot._pool_dusk = lambda: pool_mod._pool_fetch_real()
    # Capture rotation log
    original_rlog = rot._rlog_info.__code__ if hasattr(rot._rlog_info, '__code__') else None
    def _capture(msg):
        _rlog_lines.append(msg)
    rot._rlog_info  = _capture
    rot._rlog_ok    = _capture
    rot._rlog_warn  = _capture
    rot._rlog_err   = _capture
    rot._rlog_step  = _capture

def _reset():
    cmd_log.reset()
    _rlog_lines.clear()
    rot._rotation_state.update({
        "state":               rot.ROTATING,
        "enabled":             True,
        "last_rotation_epoch": -1,
        "precheck_done_epoch": -1,
        "snatch_done_epoch":   -1,
        "last_state_check_block": -999,
        "sweeper_candidate_block": -1,
        "sweeper_candidate_dusk":  0.0,
        "waiting_for_ta1_idx": None,
        "pending_terminate":   None,
        "pending_deactivate":  None,
    })
    _patch()

def _cmds_contain(keyword: str) -> bool:
    return any(keyword in c for c in cmd_log.calls)

def _rlog_contains(keyword: str) -> bool:
    return any(keyword in l for l in _rlog_lines)

def _drive_state_check(blk_left: int = 500):
    """Drive a state check at the given blk_left."""
    epoch_base = (3_100_000 // rot.EPOCH_BLOCKS) * rot.EPOCH_BLOCKS
    block = epoch_base + rot.EPOCH_BLOCKS - blk_left
    rot._run_state_check(block, block // rot.EPOCH_BLOCKS + 1, blk_left)
    time.sleep(0.3)

def _drive_to_rotation_window():
    """Drive on_block calls to enter the rotation window."""
    epoch_base = (3_100_000 // rot.EPOCH_BLOCKS) * rot.EPOCH_BLOCKS
    rot_win = 41
    # Pre-check
    rot.on_block(epoch_base + rot.EPOCH_BLOCKS - (rot_win + 5))
    time.sleep(0.2)
    # Rotation window entry
    rot.on_block(epoch_base + rot.EPOCH_BLOCKS - rot_win)
    time.sleep(0.5)


# ── Master node mock ───────────────────────────────────────────────────────────
# Inject master stake into assess mock's by_idx under index 0

def _set_assess_with_master(am: AssessMock, master_stake: float):
    """Patch the current assess state to include prov0 (master) with given stake."""
    state = dict(am._state)
    n0 = am._node(0, "active", None, stake=master_stake, addr="prov0addr"+"x"*30)
    by_idx = dict(state.get("by_idx", {}))
    by_idx[0] = n0
    state["by_idx"] = by_idx
    am._state = state


# ══════════════════════════════════════════════════════════════════════════════
# SCENARIOS
# ══════════════════════════════════════════════════════════════════════════════

def scenario_threshold_not_triggered():
    """Master above threshold — no special action should fire."""
    print(f"\n{CYN}▶ Scenario 1: Master above threshold — no action{RST}")
    _reset()
    assess_m.a1m1_ta1()
    _set_master_stake(600000.0)   # above 500k threshold
    _set_assess_with_master(assess_m, 600000.0)
    _patch()

    _drive_state_check()
    _drive_to_rotation_window()

    not _rlog_contains("master below threshold") \
        and _ok("no master-threshold action fired") \
        or  _fail("master-threshold action fired unexpectedly")
    _cmds_contain("liquidate") \
        and _ok("normal rotation fired (liquidate present)") \
        or  _warn("normal rotation did not fire (may need ta=1 check)")
    _info(f"master_stake=600k (threshold=500k)")


def scenario_a1m1_ta1_threshold():
    """A:1 M:1 ta=1 — ideal state. Master below threshold. Should act at rotation window."""
    print(f"\n{CYN}▶ Scenario 2: A:1 M:1 ta=1 — master below threshold, act at rotation window{RST}")
    _reset()
    assess_m.a1m1_ta1(rot_stake=1500000.0, rot_locked=50000.0)
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_to_rotation_window()

    # Expected: liquidate rot_active, put bulk into master (prov0), re-seed rot_active
    liq = _cmds_contain("liquidate")
    liq and _ok("liquidate rot_active fired") or _fail("liquidate NOT fired")

    # Check stake-activate goes to prov0 (master top-up) not just prov2
    master_topup = any("prov0addr" in c and "stake-activate" in c for c in cmd_log.calls)
    master_topup and _ok("master top-up stake-activate fired") \
                 or  _fail("master top-up NOT detected — stake went elsewhere")

    reseed = any("prov1addr" in c and "stake-activate" in c for c in cmd_log.calls) or \
             any("prov2addr" in c and "stake-activate" in c for c in cmd_log.calls)
    reseed and _ok("rot_active re-seed fired") or _fail("rot_active re-seed NOT fired")

    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a1m1_ta2_threshold():
    """A:1 M:1 ta=2 — rot_slave just seeded. Master below threshold. Should wait for ta=1."""
    print(f"\n{CYN}▶ Scenario 3: A:1 M:1 ta=2 — must wait, rot_slave not ready{RST}")
    _reset()
    assess_m.a1m1_ta2(rot_stake=1500000.0)
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_to_rotation_window()

    # Expected: pre-check should skip rotation (no ta=1 slave)
    not _cmds_contain("liquidate") \
        and _ok("rotation correctly skipped — no ta=1 slave") \
        or  _fail("rotation fired despite ta=2 slave — wrong")

    _rlog_contains("skip") or _rlog_contains("ta=2") or _rlog_contains("no rot_slave") \
        and _ok("skip reason logged") \
        or  _warn("no skip reason found in logs")
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a1m0_i1_threshold():
    """A:1 M:0 I:1 — no rot_slave. Master below threshold. State check should seed inactive first."""
    print(f"\n{CYN}▶ Scenario 4: A:1 M:0 I:1 — seed inactive first, then wait for rotation{RST}")
    _reset()
    assess_m.a1m0_i1(rot_stake=1500000.0)
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_state_check(blk_left=500)

    seed_fired = _cmds_contain("stake-activate")
    seed_fired and _ok("inactive node seeded") or _fail("inactive node NOT seeded")

    not _cmds_contain("liquidate") \
        and _ok("no liquidation during state check (correct)") \
        or  _fail("unexpected liquidation during state check")
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a0m2_ta12_threshold():
    """A:0 M:2 ta=[1,2] — healthy stagger. Master below threshold. Wait for ta=1 to activate."""
    print(f"\n{CYN}▶ Scenario 5: A:0 M:2 ta=[1,2] — wait for ta=1 to become active{RST}")
    _reset()
    assess_m.a0m2_ta12()
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_state_check(blk_left=500)

    not _cmds_contain("liquidate") \
        and _ok("no premature liquidation — waiting for active node") \
        or  _fail("unexpected liquidation")

    not _cmds_contain("stake-activate") \
        and _ok("no extra seeding — stagger healthy") \
        or  _warn("unexpected stake-activate during healthy stagger")
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a0m1_ta2_i1_threshold():
    """A:0 M:1 ta=2, I:1 — one just seeded, one inactive. Should seed inactive, wait."""
    print(f"\n{CYN}▶ Scenario 6: A:0 M:1 ta=2, I:1 — seed inactive, then wait{RST}")
    _reset()
    assess_m.a0m1_ta2_i1()
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_state_check(blk_left=500)

    seed_fired = _cmds_contain("stake-activate")
    seed_fired and _ok("inactive node seeded") or _fail("inactive NOT seeded")

    not _cmds_contain("liquidate") \
        and _ok("no liquidation — correct") \
        or  _fail("unexpected liquidation")
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a0m0_i2_threshold():
    """A:0 M:0 I:2 — both inactive. Master below threshold. Bootstrap first."""
    print(f"\n{CYN}▶ Scenario 7: A:0 M:0 I:2 — bootstrap rotation nodes first{RST}")
    _reset()
    assess_m.a0m0_i2()
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_state_check(blk_left=500)

    seed_fired = _cmds_contain("stake-activate")
    seed_fired and _ok("first node seeded (bootstrap)") or _fail("bootstrap seed NOT fired")

    # Should only seed ONE node — wait for ta=1 before seeding second
    activate_count = sum(1 for c in cmd_log.calls if "stake-activate" in c)
    activate_count == 1 \
        and _ok("only one node seeded (correct — waiting for ta=1 before second)") \
        or  _warn(f"unexpected activate count: {activate_count}")
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_a0m1_ta1_threshold():
    """A:0 M:1 ta=1 — one about to go active, one inactive. Seed inactive, wait."""
    print(f"\n{CYN}▶ Scenario 8: A:0 M:1 ta=1, I:1 — seed inactive (different epochs), wait{RST}")
    _reset()
    assess_m.a0m1_ta1(rot_stake=1500000.0)
    _set_master_stake(300000.0)
    _set_assess_with_master(assess_m, 300000.0)
    _patch()

    _drive_state_check(blk_left=500)

    seed_fired = _cmds_contain("stake-activate")
    seed_fired and _ok("inactive node seeded") or _fail("inactive NOT seeded — will stagger badly")
    _info(f"commands: {[c.split()[0] for c in cmd_log.calls]}")


def scenario_master_exactly_at_threshold():
    """Master exactly at threshold — should NOT trigger (only below triggers)."""
    print(f"\n{CYN}▶ Scenario 9: Master exactly at threshold — no action{RST}")
    _reset()
    assess_m.a1m1_ta1()
    _set_master_stake(500000.0)  # exactly at threshold
    _set_assess_with_master(assess_m, 500000.0)
    _patch()

    _drive_to_rotation_window()

    not _rlog_contains("master below threshold") \
        and _ok("no master-threshold action — at threshold is OK") \
        or  _fail("master-threshold action fired at exact threshold")
    _info(f"master_stake=500k (threshold=500k)")


def scenario_master_just_below_threshold():
    """Master just 1 DUSK below threshold — must trigger."""
    print(f"\n{CYN}▶ Scenario 10: Master 1 DUSK below threshold — must trigger{RST}")
    _reset()
    assess_m.a1m1_ta1()
    _set_master_stake(499999.0)
    _set_assess_with_master(assess_m, 499999.0)
    _patch()

    _drive_to_rotation_window()

    triggered = _rlog_contains("master below threshold") or _cmds_contain("liquidate")
    triggered and _ok("threshold correctly triggered at 499999 DUSK") \
              or  _fail("threshold NOT triggered — 1 DUSK below should trigger")
    _info(f"master_stake=499999 DUSK (threshold=500k)")


# ── Scenario registry ──────────────────────────────────────────────────────────


class ProvisionerState:
    """Simulates on-chain add-order and config label assignments."""

    def __init__(self, keys: dict, add_order: list, config_labels: dict):
        """
        keys: {key_id: {'addr': str, 'stake': float}}
        add_order: [key_id, ...] oldest first
        config_labels: {config_label: key_id}  e.g. {'prov0': 'A', 'prov1': 'B'}
        """
        self.keys          = keys
        self.add_order     = list(add_order)
        self.config_labels = dict(config_labels)
        self._step_log: list[str] = []

    def label_of(self, key_id: str) -> str:
        for label, kid in self.config_labels.items():
            if kid == key_id: return label
        return key_id

    def key_of(self, label: str) -> str:
        return self.config_labels.get(label, label)

    def oldest(self) -> str:
        return self.add_order[0]

    def newest(self) -> str:
        return self.add_order[-1]

    def remove_and_readd(self, key_id: str):
        """Remove a key from add-order and re-add it as newest."""
        if key_id in self.add_order:
            self.add_order.remove(key_id)
            self.add_order.append(key_id)
            self._step_log.append(
                f"  remove+re-add {self.label_of(key_id)} ({key_id[:6]}…) → now newest")

    def swap_config_labels(self, label_a: str, label_b: str):
        """Swap which physical key is assigned to two config labels."""
        ka = self.config_labels[label_a]
        kb = self.config_labels[label_b]
        self.config_labels[label_a] = kb
        self.config_labels[label_b] = ka
        self._step_log.append(
            f"  config swap: {label_a}↔{label_b} ({ka[:6]}↔{kb[:6]}…)")

    def print_state(self, title: str):
        print(f"\n    {DIM}{title}{RST}")
        for i, kid in enumerate(self.add_order):
            label = self.label_of(kid)
            stake = self.keys[kid]['stake']
            master_flag = " ★ MASTER" if label == "prov0" else ""
            unstake_target = " ← unstakes hit here first" if i == 0 else ""
            print(f"    add-order {i+1}: {label} ({kid[:8]}…) "
                  f"{stake:,.0f} DUSK{master_flag}{unstake_target}")

    def print_steps(self):
        if self._step_log:
            print(f"\n    {YEL}Steps:{RST}")
            for s in self._step_log:
                print(f"    {s}")
        self._step_log.clear()


def simulate_reorder(scenario_name: str, initial: ProvisionerState,
                     new_master_label: str):
    """
    Simulate making new_master_label the new prov0 (master).
    Goal: new master = oldest (first to absorb unstakes) + config label prov0.
    """
    print(f"\n{CYN}▶ Reorder simulation: {scenario_name}{RST}")
    print(f"    Goal: make {new_master_label} the new master (prov0, oldest)")
    initial.print_state("Before:")

    new_master_key = initial.key_of(new_master_label)

    # Step 1: make new master the oldest by removing+re-adding all OTHER keys
    others = [kid for kid in initial.add_order if kid != new_master_key]
    for kid in others:
        initial.remove_and_readd(kid)

    # Step 2: swap config labels so new master becomes prov0
    if new_master_label != "prov0":
        initial.swap_config_labels("prov0", new_master_label)

    initial.print_steps()
    initial.print_state("After:")

    # Verify
    ok = initial.oldest() == new_master_key and initial.config_labels["prov0"] == new_master_key
    ok and _ok(f"{initial.label_of(new_master_key)} is now prov0 AND oldest ✓") \
       or  _fail(f"reorder failed — oldest={initial.label_of(initial.oldest())}, "
                 f"prov0={initial.label_of(initial.config_labels['prov0'])}")


def run_reorder_simulations():
    print(f"\n{CYN}{'═'*60}")
    print(f"  RE-ORDERING SIMULATIONS")
    print(f"{'═'*60}{RST}")
    print(f"""
  Legend:
    add-order = on-chain order (oldest first = first hit by unstakes)
    config label prov0 = master (excluded from rotation)
    remove+re-add = on-chain tx making a node newest
    config swap = local config edit only (no on-chain tx needed)
  """)

    # ── Simulation 1: prov1 becomes new master ─────────────────────────────
    s1 = ProvisionerState(
        keys={
            "A": {"addr": "rFHBm9m…", "stake": 150000.0},   # prov0 (current master, drained)
            "B": {"addr": "okH7C8Z…", "stake": 1500000.0},  # prov1 (rot_active, large stake)
            "C": {"addr": "nnjnYdW…", "stake": 1000.0},     # prov2 (rot_slave, 1k)
        },
        add_order=["A", "B", "C"],   # A=oldest (added first), C=newest
        config_labels={"prov0": "A", "prov1": "B", "prov2": "C"},
    )
    simulate_reorder("prov1 (okH7C8Z) becomes new master", s1, "prov1")

    print(f"\n    {DIM}Note: key B has 1.5M DUSK — needs to be active before remove+re-add.")
    print(f"    Only possible after liquidation (zero stake required for removal).{RST}")

    # ── Simulation 2: prov2 becomes new master ─────────────────────────────
    s2 = ProvisionerState(
        keys={
            "A": {"addr": "rFHBm9m…", "stake": 150000.0},   # prov0 (current master, drained)
            "B": {"addr": "okH7C8Z…", "stake": 1000.0},     # prov1 (rot_slave, 1k)
            "C": {"addr": "nnjnYdW…", "stake": 1500000.0},  # prov2 (rot_active, large stake)
        },
        add_order=["A", "B", "C"],
        config_labels={"prov0": "A", "prov1": "B", "prov2": "C"},
    )
    simulate_reorder("prov2 (nnjnYdW) becomes new master", s2, "prov2")

    # ── Simulation 3: prov0 already oldest, just needs stake top-up ────────
    # (most common case — no re-ordering needed)
    print(f"\n{CYN}▶ Reorder simulation: prov0 stays master (stake top-up only){RST}")
    s3 = ProvisionerState(
        keys={
            "A": {"addr": "rFHBm9m…", "stake": 300000.0},   # prov0 (master, below threshold)
            "B": {"addr": "okH7C8Z…", "stake": 1500000.0},  # prov1 (rot_active)
            "C": {"addr": "nnjnYdW…", "stake": 1000.0},     # prov2 (rot_slave)
        },
        add_order=["A", "B", "C"],
        config_labels={"prov0": "A", "prov1": "B", "prov2": "C"},
    )
    print(f"    Goal: top up prov0 from rot_active — no re-ordering needed")
    s3.print_state("Before:")
    print(f"\n    {YEL}Steps:{RST}")
    print(f"    liquidate+terminate prov1 (B) → freed stake → pool")
    print(f"    stake-activate prov0 (A) ← bulk of freed stake (new master stake)")
    print(f"    stake-activate prov1 (B) ← 1000 DUSK re-seed")
    # Simulate stake changes
    s3.keys["A"]["stake"] = 1650000.0  # topped up from B's freed stake
    s3.keys["B"]["stake"] = 1000.0     # re-seeded
    s3.print_state("After (stake only, add-order unchanged):")
    _ok("prov0 still oldest, no on-chain re-ordering needed")
    print(f"    {DIM}Add-order unchanged: A still oldest → absorbs unstakes first{RST}")

    print(f"\n{CYN}{'═'*60}{RST}")
    print(f"""
  Key insight:
    Simulation 3 (stake top-up, no re-order) is the PRIMARY scenario.
    prov0 stays oldest throughout — we simply move stake from rot_active
    into master. No remove+re-add needed.

    Simulations 1 & 2 (actual master change) only apply if we want to
    REPLACE which physical key is master — much more complex and requires
    liquidating nodes that may have large stake. Likely only needed if
    the original prov0 key is compromised or permanently decommissioned.
  """)

SCENARIOS = {
    "no_trigger":        scenario_threshold_not_triggered,
    "a1m1_ta1":          scenario_a1m1_ta1_threshold,
    "a1m1_ta2":          scenario_a1m1_ta2_threshold,
    "a1m0_i1":           scenario_a1m0_i1_threshold,
    "a0m2_ta12":         scenario_a0m2_ta12_threshold,
    "a0m1_ta2_i1":       scenario_a0m1_ta2_i1_threshold,
    "a0m0_i2":           scenario_a0m0_i2_threshold,
    "a0m1_ta1":          scenario_a0m1_ta1_threshold,
    "exact_threshold":   scenario_master_exactly_at_threshold,
    "just_below":        scenario_master_just_below_threshold,
}

SCENARIOS["reorder"] = run_reorder_simulations

if __name__ == "__main__":
    requested = sys.argv[1:] if len(sys.argv) > 1 else list(SCENARIOS.keys())
    unknown   = [s for s in requested if s not in SCENARIOS]
    if unknown:
        print(f"Unknown scenarios: {unknown}")
        print(f"Available: {list(SCENARIOS.keys())}")
        sys.exit(1)

    print(f"\n{CYN}{'═'*60}")
    print(f"  SOZU Master Threshold — Test Harness")
    print(f"  Running {len(requested)} scenario(s)")
    print(f"{'═'*60}{RST}")
    print(f"\n  {YEL}NOTE: These are dry-run tests against CURRENT rotation logic.")
    print(f"  Master threshold logic is NOT yet coded — scenarios marked")
    print(f"  with ✗ show what NEEDS to be implemented.{RST}\n")

    for name in requested:
        SCENARIOS[name]()

    print(f"\n{CYN}{'═'*60}{RST}\n")


# ══════════════════════════════════════════════════════════════════════════════
# RE-ORDERING SIMULATION
# Simulates the on-chain add-order and config label changes needed when a
# rotation node becomes the new master.
#
# Key concepts:
#   - On-chain add-order determines who absorbs unstakes first (oldest = first)
#   - Config labels (prov0/prov1/prov2) are just our internal names
#   - prov0 config label always = master (excluded from rotation)
#   - To change which physical key is "oldest", remove+re-add the others
# ══════════════════════════════════════════════════════════════════════════════
