"""
nodes.py — Minimal local node block-height monitor.

One thread per provisioner node connects to its local rusk WebSocket,
subscribes to blocks/accepted, and updates _node_heights on each block.

No RUES event routing. No rotation callbacks. Just heights.
"""
import json
import threading
import time

from .config import _log, RUSK_VERSION, NODE_INDICES, cfg, _default_ws_port, _default_log_path

_node_heights: dict = {i: None for i in NODE_INDICES}
_node_heights_lock  = threading.Lock()


def get_heights() -> dict:
    with _node_heights_lock:
        return dict(_node_heights)


# [consensus_key] The BLS key a node really signs with, read from its rusk log
# (consensus spans: main{round=… iter=… name=Validation pk="ua4Sc1pBUyBqbPmz"}).
# rusk logs only a 16-char prefix; the dashboard compares it against the
# configured prov_N_address to catch a mis-pasted address or a wrong key export.
import re as _re
_PK_RE            = _re.compile(r'main\{round=\d+ iter=\d+ name=\w+ pk="([^"]+)"\}')
# rusk writes its log with tracing's ANSI styling (bold span names, italic
# field names, dimmed "="), invisible in `tail -f` but it breaks the match.
_ANSI_RE          = _re.compile(r'\x1b\[[0-9;]*m')
_PK_TAIL_BYTES    = 2 * 1024 * 1024
_PK_CACHE_SECS    = 30
_pk_cache: dict   = {}   # idx → {"pk", "round", "ts"}
_pk_cache_lock    = threading.Lock()


def _read_consensus_key(path: str):
    """Last consensus pk in the tail of a rusk log, as (pk, round) or (None, None)."""
    import os
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        f.seek(max(0, f.tell() - _PK_TAIL_BYTES))
        tail = _ANSI_RE.sub("", f.read().decode("utf-8", "replace"))
    last = None
    for last in _PK_RE.finditer(tail):
        pass
    if not last:
        return None, None
    rnd = _re.search(r"round=(\d+)", last.group(0))
    return last.group(1), int(rnd.group(1)) if rnd else None


def get_consensus_keys() -> dict:
    """idx → {"pk", "round", "error"} for every node, cached for _PK_CACHE_SECS."""
    now = time.time()
    out = {}
    for idx in NODE_INDICES:
        with _pk_cache_lock:
            hit = _pk_cache.get(idx)
        if hit and now - hit["ts"] < _PK_CACHE_SECS:
            out[idx] = {k: v for k, v in hit.items() if k != "ts"}
            continue
        path = cfg(f"node_{idx}_log") or _default_log_path(idx)
        entry = {"pk": None, "round": None, "error": None}
        try:
            entry["pk"], entry["round"] = _read_consensus_key(path)
        except Exception as e:
            entry["error"] = f"{type(e).__name__}: {e}"
        with _pk_cache_lock:
            _pk_cache[idx] = {**entry, "ts": now}
        out[idx] = entry
    return out


def _on_remote_block(height: int) -> None:
    """Called by rues.py when a block_accepted event carries a height.
    Stored separately so the dashboard can display remote tip alongside local heights."""
    global _remote_height
    _remote_height = height


_remote_height: int = 0


def get_remote_height() -> int:
    return _remote_height


def _ws_thread(node_idx: int) -> None:
    try:
        import websocket as _ws
    except ImportError:
        _log(f"[node{node_idx}] websocket-client not installed — height unavailable")
        return

    backoff = 2
    while True:
        port = int(cfg(f"node_{node_idx}_ws_port") or _default_ws_port(node_idx))  # [six_nodes]
        url  = f"ws://localhost:{port}/on"
        try:
            ws = _ws.create_connection(url, timeout=10, ping_interval=0)

            # Receive session ID
            raw = ws.recv()
            session_id = (raw.decode("utf-8") if isinstance(raw, bytes) else raw).strip().strip('"')
            _log(f"[node{node_idx}] connected session={session_id[:16]}…")

            # Subscribe to blocks/accepted  (424 = auto-subscribed by local node, fine)
            import urllib.request as _ur, urllib.error as _uerr
            sub_url = f"http://localhost:{port}/on/blocks/accepted"
            try:
                req = _ur.Request(sub_url, method="GET",
                                  headers={"Rusk-Session-Id": session_id,
                                           "rusk-version": RUSK_VERSION})
                with _ur.urlopen(req, timeout=6) as r:
                    r.read()
            except _uerr.HTTPError as he:
                if he.code != 424:
                    _log(f"[node{node_idx}] subscribe failed HTTP {he.code}")
                    ws.close(); time.sleep(backoff); backoff = min(backoff*2, 60); continue
            except Exception as se:
                _log(f"[node{node_idx}] subscribe failed: {se}")
                ws.close(); time.sleep(backoff); backoff = min(backoff*2, 60); continue

            backoff   = 2
            last_ping = time.time()
            _log(f"[node{node_idx}] subscribed to blocks/accepted")

            while True:
                # Keepalive ping every 30s
                if time.time() - last_ping > 30:
                    try:
                        ws.ping(); last_ping = time.time()
                    except Exception:
                        break

                ws.settimeout(5)
                try:
                    raw = ws.recv()
                except _ws.WebSocketTimeoutException:
                    continue
                except Exception:
                    break

                if not raw:
                    continue

                # Parse the RUES binary frame: find JSON header then payload
                raw_bytes = raw if isinstance(raw, bytes) else raw.encode()
                brace = raw_bytes.find(b'{')
                if brace == -1:
                    continue
                json_bytes = raw_bytes[brace:]
                depth, hdr_end = 0, -1
                for i, b in enumerate(json_bytes):
                    if b == ord('{'): depth += 1
                    elif b == ord('}'): depth -= 1
                    if depth == 0:
                        hdr_end = i; break
                if hdr_end == -1:
                    continue
                try:
                    hdr     = json.loads(json_bytes[:hdr_end+1])
                    payload = json_bytes[hdr_end+1:]
                except Exception:
                    continue

                if "/blocks:" not in hdr.get("Content-Location", ""):
                    continue

                try:
                    decoded = json.loads(payload.decode("utf-8", errors="replace").strip())
                    # Try both nested and flat payload formats
                    h = decoded.get("header") or {}
                    height = (int(h.get("height", 0) or 0)
                              or int(decoded.get("height", 0) or 0))
                    if height:
                        with _node_heights_lock:
                            _node_heights[node_idx] = height
                        _log(f"[node{node_idx}] height={height}")
                        try:
                            from .rotation import on_block as _on_block
                            _on_block(height)
                        except Exception as _e:
                            _log(f"[node{node_idx}] on_block error: {_e}")
                except Exception:
                    pass

            try:
                ws.close()
            except Exception:
                pass

        except Exception as e:
            _log(f"[node{node_idx}] connection error: {e}")

        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def start_node_monitors() -> None:
    """Start one height-monitor thread per provisioner node."""
    for idx in NODE_INDICES:
        threading.Thread(target=_ws_thread, args=(idx,),
                         daemon=True, name=f"node{idx}_ws").start()
    _log("[nodes] local height monitors started")
