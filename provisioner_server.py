"""
server.py — Entry point.

systemd / gunicorn:
    gunicorn --workers 1 --bind 0.0.0.0:7373 server:app
Direct:
    python3 server.py
"""
import functools
import os
import secrets

from flask import Flask, Response, request

from lib.config import (
    _log, configure_werkzeug_logger,
    NETWORK, CONTRACT_ID, WALLET_PATH, PORT,
    _load_config, cfg,
)

app = Flask(__name__)
# No CORS: the dashboard is served same-origin by this app (see routes/system.py
# index()), so no cross-origin access is needed. A wildcard Access-Control-
# Allow-Origin would invite any website to call the money-moving API.
configure_werkzeug_logger()

# ── Auth (fail closed) ─────────────────────────────────────────────────────────
# The dashboard drives stake-moving endpoints, so it must never come up
# unauthenticated by accident. If the credentials are unset we refuse to start,
# unless SOZU_ALLOW_NO_AUTH=1 is set as a deliberate, documented override (only
# ever acceptable on a loopback-only bind behind a tunnel — never on 0.0.0.0).
_AUTH_USER     = os.environ.get("SOZU_DASHBOARD_USER", "")
_AUTH_PASS     = os.environ.get("SOZU_DASHBOARD_PASS", "")
_ALLOW_NO_AUTH = os.environ.get("SOZU_ALLOW_NO_AUTH") == "1"

if not (_AUTH_USER and _AUTH_PASS) and not _ALLOW_NO_AUTH:
    raise SystemExit(
        "FATAL: SOZU_DASHBOARD_USER / SOZU_DASHBOARD_PASS are not set — refusing "
        "to start without authentication. Set both, or set SOZU_ALLOW_NO_AUTH=1 "
        "to override (never on a network-reachable bind)."
    )

_AUTH_ENABLED = bool(_AUTH_USER and _AUTH_PASS)


def _check_auth(username: str, password: str) -> bool:
    ok_user = secrets.compare_digest(username.encode(), _AUTH_USER.encode())
    ok_pass = secrets.compare_digest(password.encode(), _AUTH_PASS.encode())
    return ok_user and ok_pass


@app.before_request
def _global_auth():
    if not _AUTH_ENABLED or request.method == "OPTIONS":
        return
    auth = request.authorization
    if not auth or not _check_auth(auth.username, auth.password):
        return Response("Authentication required.", 401,
                        {"WWW-Authenticate": 'Basic realm="SOZU Dashboard"'})


from lib.routes.system    import bp as system_bp
from lib.routes.info      import bp as info_bp
from lib.routes.actions   import bp as actions_bp
from lib.routes.substrate import bp as substrate_bp

app.register_blueprint(system_bp)
app.register_blueprint(info_bp)
app.register_blueprint(actions_bp)
app.register_blueprint(substrate_bp)

_started = False


@app.before_request
def _ensure_started():
    global _started
    if not _started:
        _started = True
        _load_config()
        from lib.nodes import start_node_monitors
        start_node_monitors()
        from lib.rues import start as start_rues
        start_rues()
        from lib.rues import start
        start()


if __name__ == "__main__":
    _load_config()
    print(f"  Provisioner Manager API  ->  http://localhost:{PORT}")
    print(f"  Wallet: {WALLET_PATH}  |  Network: {NETWORK}")
    print(f"  Contract: {CONTRACT_ID[:20]}...")
    if _AUTH_ENABLED:
        print(f"  Auth: enabled  (user: {_AUTH_USER})")
    else:
        print(f"  Auth: disabled")
    print()
    from lib.nodes import start_node_monitors
    start_node_monitors()
    from lib.rues import start as rues_start
    rues_start()
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)
