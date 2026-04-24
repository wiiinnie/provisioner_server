#!/usr/bin/env python3
"""
config_patch.py — Add v2 heal config keys to lib/config.py.

Adds 5 new keys to _CONFIG_DEFAULTS:
  - master_heal_enabled         (bool,  True)   — master-heal auto-trigger enabled
  - master_alert_threshold_pct  (float, 70.0)   — TG alert fires at X% of target_master
  - master_heal_threshold_pct   (float, 50.0)   — heal triggers at X% of target_master
  - rotation_floor_pct          (float, 20.0)   — rot_active target = max_cap × X%
  - max_harvest_deferrals       (int,   3)      — consecutive defers before force-run

Note: These REPLACE the old `master_threshold_pct` which was applied against
max_capacity. The new thresholds use target_master as base (target_master =
max_cap × (1 - rotation_floor_pct/100)). The dashboard HTML migrates the old
value automatically on first load.

Idempotent: safe to re-run. Skips keys that already exist.

Usage (from repo root):
    python3 config_patch.py
"""

import re
import shutil
import os
import sys
from datetime import datetime

TARGET = "lib/config.py"

NEW_KEYS = [
    ('master_heal_enabled',         'True',   '# master-heal auto-trigger enabled'),
    ('master_alert_threshold_pct',  '70.0',   '# TG alert at X% of target_master'),
    ('master_heal_threshold_pct',   '50.0',   '# heal triggers at X% of target_master'),
    ('rotation_floor_pct',          '20.0',   '# rot_active target = max_cap × X %'),
    ('max_harvest_deferrals',       '3',      '# defers when unstake-target; force-run at cap'),
]


def main():
    if not os.path.exists(TARGET):
        print(f"✖ {TARGET} not found. Run from repo root.", file=sys.stderr)
        sys.exit(1)

    with open(TARGET) as f:
        src = f.read()

    # Find an anchor — prefer master_threshold_pct if still present, else master_idx
    anchor_re = re.compile(
        r'(^\s*"(?:master_threshold_pct|master_idx)":\s*[^,]+,\s*(?:#[^\n]*)?\s*\n)',
        re.MULTILINE)
    m = anchor_re.search(src)
    if not m:
        print(f"✖ Could not find anchor line in {TARGET}. Expected one of: "
              f"master_threshold_pct, master_idx.", file=sys.stderr)
        sys.exit(1)

    anchor_line = m.group(1)
    indent_match = re.match(r'(\s*)"', anchor_line)
    indent = indent_match.group(1) if indent_match else "    "

    # Build insertion block — only keys not already present
    to_add = []
    for key, val, comment in NEW_KEYS:
        if re.search(rf'"{re.escape(key)}":', src):
            print(f"  = '{key}' already present, skipping")
            continue
        padding = " " * max(1, 28 - len(key))
        to_add.append(f'{indent}"{key}":{padding}{val},  {comment}')

    if not to_add:
        print("✓ All keys already present, nothing to do.")
        return

    # Backup
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = f"{TARGET}.bak-config-patch-{ts}"
    shutil.copy(TARGET, backup)
    print(f"✓ Backed up to {backup}")

    # Insert block after the anchor line
    insertion = "\n".join(to_add) + "\n"
    end_of_anchor = m.end()
    new_src = src[:end_of_anchor] + insertion + src[end_of_anchor:]

    with open(TARGET, "w") as f:
        f.write(new_src)

    print(f"✓ Added {len(to_add)} key(s) to {TARGET}")
    for line in to_add:
        print(f"    + {line.strip()}")


if __name__ == "__main__":
    main()
