"""
wallet.py — Shell command execution, wallet/operator cmd wrappers, password cache.

Imports: config only.
"""
import os
import re
import shlex
import signal
import subprocess
import threading
import time
from datetime import datetime

from .config import (
    _log, WALLET_BIN, WALLET_PATH, OPERATOR_WALLET,
    _NET, RUSK_VERSION, GAS_LIMIT,
)

# ── Locks ─────────────────────────────────────────────────────────────────────
# _wallet_lock:   general wallet commands (balance, profiles, stake-info)
# _rotation_lock: time-critical pool commands (stake-activate, liquidate, deactivate)
#                 kept separate so dashboard reads never block fast-alloc
_wallet_lock   = threading.Lock()
_rotation_lock = threading.Lock()

# ── Secret redaction ───────────────────────────────────────────────────────────
# Masks wallet secrets in any command string before it is logged or returned to
# a client. Two mechanisms, applied together so a miss in one is caught by the
# other:
#   1. value-based — replace the literal password wherever it appears. This is
#      what catches --provisioner-password '<pw>', which the callers inline
#      UNmasked (the outer --password was masked, this one was not).
#   2. flag-based  — mask the argument that follows --password /
#      --provisioner-password even when the value is unknown (e.g. inside
#      run_cmd, which never sees the password variable).
_SECRET_FLAG_RE = re.compile(
    r"(--(?:provisioner-)?password[ =]+)('(?:[^']|'\\'')*'|\"[^\"]*\"|\S+)"
)

def _redact_secrets(text: str, password: str = "") -> str:
    if not text:
        return text
    if password and len(password) >= 4:
        text = text.replace(password, "***")
    return _SECRET_FLAG_RE.sub(r"\1'***'", text)


# ── Password cache ─────────────────────────────────────────────────────────────
# Two independent sources, deliberately kept apart:
#   • _credential_pw — loaded once from the systemd LoadCredentialEncrypted mount.
#     This is the headless/automation password; persistent by design so the
#     rotation loop can sign every epoch with no human present.
#   • _session_pw    — supplied by an authenticated dashboard request. TTL-bound
#     so a password typed once is not reusable indefinitely by a later (e.g.
#     unauthenticated / CSRF) caller. Refreshed on each supply.
_credential_pw: str   = ""
_credential_loaded    = False
_session_pw: str      = ""
_session_pw_ts: float = 0.0
_pw_lock              = threading.Lock()

# Session-password lifetime, seconds. 0 or negative disables expiry (back-compat).
try:
    _SESSION_PW_TTL = int(os.environ.get("SOZU_PW_CACHE_TTL_SEC", "3600"))
except ValueError:
    _SESSION_PW_TTL = 3600


def _load_password_from_systemd_credential() -> str:
    """
    Read the wallet password from systemd's runtime credential directory if
    present. The encrypted credential is provisioned via the unit's
    LoadCredentialEncrypted= directive; systemd decrypts it into a tmpfs path
    pointed to by $CREDENTIALS_DIRECTORY at service start.

    Returns "" if no credential is mounted (e.g. running outside systemd).
    """
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    if not cred_dir:
        return ""
    cred_file = os.path.join(cred_dir, "wallet_pw")
    if not os.path.exists(cred_file):
        return ""
    try:
        with open(cred_file) as f:
            return f.read().strip()
    except Exception:
        return ""


def _session_pw_fresh() -> bool:
    if not _session_pw:
        return False
    if _SESSION_PW_TTL <= 0:
        return True
    return (time.time() - _session_pw_ts) < _SESSION_PW_TTL


def get_password() -> str:
    """
    Resolve the wallet password.

    Order:
      1. A password carried on the current authenticated request (body
         `password` or `X-Wallet-Password` header) — cached as the session
         password and returned.
      2. A fresh session password supplied earlier (within TTL).
      3. The persistent systemd-credential password (headless mode).
    """
    global _credential_pw, _credential_loaded, _session_pw, _session_pw_ts

    if not _credential_loaded:
        with _pw_lock:
            if not _credential_loaded:
                _credential_pw = _load_password_from_systemd_credential()
                _credential_loaded = True

    try:
        from flask import has_request_context, request
        if has_request_context():
            data = request.get_json(silent=True) or {}
            pw = data.get("password", "") or request.headers.get("X-Wallet-Password", "")
            if pw:
                with _pw_lock:
                    _session_pw = pw
                    _session_pw_ts = time.time()
                return pw
    except Exception:
        pass

    if _session_pw_fresh():
        return _session_pw
    return _credential_pw


def _cache_wallet_pw(pw: str) -> None:
    global _session_pw, _session_pw_ts
    if pw:
        with _pw_lock:
            _session_pw = pw
            _session_pw_ts = time.time()


def clear_password() -> None:
    """Drop the session password (logout). The systemd-credential password is
    left intact so headless automation keeps working."""
    global _session_pw, _session_pw_ts
    with _pw_lock:
        _session_pw = ""
        _session_pw_ts = 0.0

# ── ANSI stripping ─────────────────────────────────────────────────────────────
def _strip_ansi(s: str) -> str:
    return re.sub("\x1b[^a-zA-Z]*[a-zA-Z]|\r", "", s)

# ── Input validation ──────────────────────────────────────────────────────────
# Boundary checks for values that get interpolated into a command. With
# shell=False (below) these are defence-in-depth, not the sole barrier, but they
# reject junk early and give callers a clean 400 instead of a wallet-CLI error.
_ADDR_RE = re.compile(r"^[A-Za-z0-9]{40,120}$")
_HEX_RE  = re.compile(r"^[0-9a-fA-F]{32,128}$")

def valid_addr(a: str) -> bool:
    return bool(a and _ADDR_RE.match(a))

def valid_hex(h: str) -> bool:
    return bool(h and _HEX_RE.match(h))

# ── run_cmd ───────────────────────────────────────────────────────────────────
def run_cmd(cmd: str, timeout: int = 30) -> dict:
    """Run a command WITHOUT a shell and return
    {ok, stdout, stderr, returncode, duration_ms}.

    The command string is tokenised with shlex.split and executed via
    shell=False, so metacharacters (; | & $() ` etc.) injected into an
    interpolated value are inert — they become literal argv tokens the wallet
    CLI simply rejects, never shell syntax. start_new_session=True keeps the
    process-group kill-on-timeout behaviour.
    """
    t0 = time.time()
    try:
        argv = shlex.split(cmd)
    except ValueError as exc:
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": f"invalid command quoting: {exc}",
                "duration_ms": 0,
                "cmd": _redact_secrets(cmd), "ts": datetime.now().isoformat()}
    if not argv:
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": "empty command", "duration_ms": 0,
                "cmd": _redact_secrets(cmd), "ts": datetime.now().isoformat()}
    try:
        proc = subprocess.Popen(
            argv, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
    except OSError as exc:
        # e.g. binary not found / not executable. shell=True used to surface
        # this as a non-zero return; keep that shape instead of raising.
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": f"failed to launch: {exc}", "duration_ms": 0,
                "cmd": _redact_secrets(cmd), "ts": datetime.now().isoformat()}
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return {
            "ok":          proc.returncode == 0,
            "stdout":      _strip_ansi(stdout).strip(),
            "stderr":      _strip_ansi(stderr).strip(),
            "returncode":  proc.returncode,
            "duration_ms": int((time.time() - t0) * 1000),
            "cmd":         _redact_secrets(cmd),
            "ts":          datetime.now().isoformat(),
        }
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        proc.communicate()
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": f"timeout after {timeout}s",
                "duration_ms": timeout * 1000,
                "cmd": _redact_secrets(cmd), "ts": datetime.now().isoformat()}
    except Exception as exc:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        return {"ok": False, "stdout": "", "returncode": -1,
                "stderr": str(exc), "duration_ms": 0,
                "cmd": _redact_secrets(cmd), "ts": datetime.now().isoformat()}

# ── wallet_cmd ────────────────────────────────────────────────────────────────
def wallet_cmd(subcmd: str, timeout: int = 30, password: str = "", gas_limit: int = 0) -> dict:
    """Run a command using the PROVISIONER wallet (~/sozu_provisioner)."""
    from .config import GAS_PRICE, GAS_LIMIT
    gp_flag      = f" --gas-price {GAS_PRICE()}"
    effective_gl = gas_limit if gas_limit > 0 else GAS_LIMIT()
    gl_flag      = f" --gas-limit {effective_gl}"
    if not password:
        password = get_password()
    if password:
        safe_pw = password.replace("'", "'\\''")
        inner   = f"{WALLET_BIN} {_NET} --password '{safe_pw}'{gp_flag}{gl_flag} -w {WALLET_PATH} {subcmd}"
    else:
        inner   = f"{WALLET_BIN} {_NET}{gp_flag}{gl_flag} -w {WALLET_PATH} {subcmd}"
    with _wallet_lock:
        result = run_cmd(inner, timeout=timeout)
    # Redact against the real command so any secret in `subcmd` (e.g.
    # --provisioner-password) is masked too, not just the outer --password flag.
    result["cmd"]    = _redact_secrets(inner, password)
    # Defence in depth: mask the password if the CLI echoes its argv back on error.
    result["stdout"] = _redact_secrets(result.get("stdout", ""), password)
    result["stderr"] = _redact_secrets(result.get("stderr", ""), password)
    return result

# ── operator_cmd ──────────────────────────────────────────────────────────────
_own_tx_hashes: set       = set()
_own_tx_hashes_lock       = threading.Lock()
_cmd_log: list            = []
_cmd_log_lock             = threading.Lock()

def _extract_tx_hash(output: str) -> str:
    m = re.search(r"Hash:\s*([0-9a-fA-F]{32,})", output)
    return m.group(1).strip() if m else ""

def _push_cmd_output(name: str, result: dict) -> None:
    """Store background command result for the dashboard Command Output panel."""
    from datetime import datetime as _dt
    entry = {
        "name":        name,
        "ok":          result.get("ok", False),
        "stdout":      result.get("stdout", ""),
        "stderr":      result.get("stderr", ""),
        "duration_ms": result.get("duration_ms", 0),
        "ts":          _dt.now().strftime("%H:%M:%S"),
    }
    with _cmd_log_lock:
        _cmd_log.append(entry)
        if len(_cmd_log) > 200:
            del _cmd_log[:-200]

def operator_cmd(subcmd: str, timeout: int = 30, password: str = "",
                 gas_limit: int = 0) -> dict:
    """Run a command using the OPERATOR wallet (~/sozu_operator).

    Lock selection:
      pool / stake-info / substrate → _rotation_lock  (time-critical)
      everything else               → _wallet_lock
    """
    from .config import GAS_PRICE, GAS_LIMIT
    gp_flag      = f" --gas-price {GAS_PRICE()}"
    effective_gl = gas_limit if gas_limit > 0 else GAS_LIMIT()
    gl_flag      = f" --gas-limit {effective_gl}"
    if not password:
        password = get_password()
    if password:
        safe_pw = password.replace("'", "'\\''")
        cmd     = f"{WALLET_BIN} {_NET} --password '{safe_pw}'{gp_flag}{gl_flag} -w {OPERATOR_WALLET} {subcmd}"
    else:
        cmd     = f"{WALLET_BIN} {_NET}{gp_flag}{gl_flag} -w {OPERATOR_WALLET} {subcmd}"
    # Redact against the real command so any secret in `subcmd` (e.g.
    # --provisioner-password) is masked too, not just the outer --password flag.
    log_cmd = _redact_secrets(cmd, password)

    _stripped = subcmd.strip()
    if _stripped.startswith("-n "):
        parts = _stripped.split(None, 2)
        _stripped = parts[2] if len(parts) > 2 else _stripped
    _use_rot_lock = any(_stripped.startswith(p) for p in ("pool ", "stake-info", "substrate "))
    _lock = _rotation_lock if _use_rot_lock else _wallet_lock

    with _lock:
        result = run_cmd(cmd, timeout=timeout)
    result["cmd"] = log_cmd
    # Defence in depth: mask the password if the CLI echoes its argv back on
    # error, before the output is returned or pushed to the command panel.
    result["stdout"] = _redact_secrets(result.get("stdout", ""), password)
    result["stderr"] = _redact_secrets(result.get("stderr", ""), password)

    _tx_hash = _extract_tx_hash(result.get("stdout", "") + result.get("stderr", ""))
    if _tx_hash:
        with _own_tx_hashes_lock:
            _own_tx_hashes.add(_tx_hash.lower())
            if len(_own_tx_hashes) > 500:
                _own_tx_hashes.discard(next(iter(_own_tx_hashes)))

    import threading as _thr
    if _thr.current_thread().name == "rotation":
        _push_cmd_output(subcmd.split()[0], result)
    return result

def extract_payload(output: str) -> str:
    """Extract bare hex payload from wallet calculate-payload output."""
    candidates = re.findall(r"[0-9a-fA-F]{40,}", output)
    if candidates:
        return max(candidates, key=len)
    for line in reversed(output.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            line = line.split(":", 1)[1].strip()
        line = line.strip('"').strip()
        if re.match(r"^[0-9a-fA-F]{40,}$", line):
            return line
    return ""
