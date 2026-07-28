"""
batchq.py — [batch] sidecar metadata for the sozu-wallet transaction batch.

The wallet CLI owns the actual queue ({wallet-dir}/batch.json — signed txs,
nonce-ordered). This module keeps a parallel human-readable description of
what the dashboard queued (action, provisioner, amount) so the batch panel
can render meaningful rows. The CLI remains the source of truth: entries
queued outside the dashboard won't appear here, which is why the panel also
shows the raw `batch` CLI output.

Mutations mirror the CLI's model: append on queue, pop-last on `batch remove`,
clear on `batch clear` / successful `batch propagate`.
"""
import json
import os
import threading
from datetime import datetime

_META_FILE = os.path.expanduser("~/.sozu_batch_meta.json")
_lock = threading.Lock()


def _load() -> list:
    try:
        with open(_META_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(entries: list) -> None:
    try:
        tmp = _META_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(entries, f, indent=1)
        os.replace(tmp, _META_FILE)
    except Exception:
        pass


def record(action: str, prov_idx=None, provisioner: str = "",
           amount_dusk=None) -> None:
    entry = {"ts": datetime.now().isoformat(timespec="seconds"), "action": action}
    if prov_idx is not None:
        entry["prov_idx"] = prov_idx
    if provisioner:
        entry["provisioner"] = provisioner
    if amount_dusk is not None:
        entry["amount_dusk"] = amount_dusk
    with _lock:
        entries = _load()
        entries.append(entry)
        _save(entries)


def pop_last() -> None:
    with _lock:
        entries = _load()
        if entries:
            entries.pop()
            _save(entries)


def clear() -> None:
    with _lock:
        _save([])


def entries() -> list:
    with _lock:
        return _load()
