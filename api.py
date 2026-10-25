"""HTTP surface: /register, /link, /health, /sync, /events, /a/<fire-id>.

Every request except `/register` (which mints a tenant) and `/a/<fire-id>` (the
notification's click-through page, which carries no data) is scoped to the
tenant that owns the presented key. There is no ambient tenant and no shared
key: the one key in `config.json` survives only as tenant `main` so that
already-installed builds keep syncing until they are updated.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

VERSION = "2"

# How long one /events call may hold the connection open. Cloudflare keeps a
# proxied request alive far longer than this, and a bounded wait means a client
# that vanishes costs one thread for at most this long.
EVENTS_MAX_WAIT = 30
EVENTS_POLL_SECONDS = 0.5
# A sync push is a few KB; anything past this is refused rather than buffered.
MAX_BODY = 4 * 1024 * 1024

# Signing up is free, so it needs a ceiling that is not the client's goodwill.
REGISTER_PER_HOUR = 10
REGISTER_HOURLY_CAP = 200
TENANT_CAP = 5000
GOOGLE_TOKENINFO = "https://oauth2.googleapis.com/tokeninfo?id_token="


class RateLimiter:
    """Fixed-window counter per key, in memory: a restart is not a bypass worth
    defending against, and a table would be more code than the thing it guards."""

    def __init__(self, limit: int, window: int = 3600) -> None:
        self.limit = limit
        self.window = window
        self._hits: dict[str, tuple[int, int]] = {}
        self._lock = Lock()

    def allow(self, key: str) -> bool:
        now = int(time.time())
        with self._lock:
            start, count = self._hits.get(key, (now, 0))
            if now - start >= self.window:
                start, count = now, 0
            if count >= self.limit:
                self._hits[key] = (start, count)
                return False
            self._hits[key] = (start, count + 1)
            if len(self._hits) > 4096:  # bounded memory under a spray of IPs
                self._hits = {k: v for k, v in self._hits.items() if now - v[0] < self.window}
            return True


class App:
    """Everything the request handler needs, in one place."""

    def __init__(self, store, cfg: dict, data_dir: str) -> None:
        self.store = store
        self.cfg = cfg
        self.data_dir = data_dir
        self.started_at = time.time()
        self.last_tick_ms = int(time.time() * 1000)
        self.last_sync_ms: int | None = None
        self.last_error: str | None = None
        self.register_limiter = RateLimiter(REGISTER_PER_HOUR)
        self.register_window = RateLimiter(REGISTER_HOURLY_CAP)
        self._lock = Lock()
        # Pre-v33 devices still present the old shared key.
        self.store.ensure_legacy_key(str(cfg.get("key") or ""))

    def note_error(self, message: str) -> None:
        with self._lock:
            self.last_error = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {message}"

    def tunnel_url(self) -> str | None:
        url = str(self.cfg.get("public_url") or "").strip()
        if url:
            return url
        try:
            with open(f"{self.data_dir}/url.txt", encoding="utf-8") as handle:
                url = handle.read().strip()
        except OSError:
            return None
        return url or None

    def health(self, tenant: str) -> dict:
        alerts = self.store.alerts(tenant)
        armed = [a for a in alerts if a.get("armed")]
        bar_close = [a for a in armed if a.get("cadence") == "barClose"]
        counts = self.store.counts(tenant)
        return {
            "ok": True,
            "version": VERSION,
            "tenant": tenant,
            "uptimeSec": int(time.time() - self.started_at),
            "lastTickMs": self.last_tick_ms,
            "lastSyncMs": self.last_sync_ms,
            "alerts": counts["alerts"],
            "armed": len(armed),
            "serverSide": len(bar_close),
            "appOnly": len(armed) - len(bar_close),
            "fires": counts["fires"],
            "journal": counts["journal"],
            "cursors": self.store.cursors(),
            "dbKB": self.store.size_kb(),
            "db": "postgres" if self.store.is_durable else "sqlite",
            "tenants": self.store.tenant_count(),
            "tunnel": self.tunnel_url(),
            "lastError": self.last_error,
        }

    def register(self, client_ip: str) -> tuple[int, dict]:
        if not self.register_limiter.allow(client_ip):
            return 429, {"error": "too many sign-ups from this address"}
        if not self.register_window.allow("global"):
            return 429, {"error": "relay is at capacity, try later"}
        if self.store.tenant_count() >= TENANT_CAP:
            return 503, {"error": "relay is at capacity"}
        tenant, key = self.store.create_tenant()
        print(f"registered tenant {tenant} from {client_ip}", flush=True)
        return 201, {
            "tenant": tenant,
            "key": key,
            "now": int(time.time() * 1000),
            "server": {"version": VERSION},
        }

    def verify_google(self, id_token: str) -> dict | None:
        """Ask Google, rather than verifying a JWT in-process.

        The relay has outbound network and no crypto library; the tokeninfo
        endpoint is the documented way to check an ID token's audience and
        signature, and it is called once per sign-in, not per request.
        """
        client_id = str(self.cfg.get("google_client_id") or "").strip()
        if not client_id:
            return None
        url = GOOGLE_TOKENINFO + urllib.parse.quote(id_token, safe="")
        try:
            with urllib.request.urlopen(url, timeout=8) as response:
                info = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            # 400/401 from tokeninfo means the token itself is bad: a client
            # mistake, not a relay fault, so it must not raise /health's alarm.
            if error.code not in (400, 401):
                self.note_error(f"google tokeninfo: {error}")
            return None
        except (urllib.error.URLError, ValueError, OSError) as error:
            self.note_error(f"google tokeninfo: {error}")
            return None
        if not isinstance(info, dict):
            return None
        if info.get("aud") != client_id:
            return None
        if str(info.get("email_verified", "")).lower() != "true":
            return None
        if not info.get("sub"):
            return None
        return info

    def link(self, tenant: str, id_token: str) -> tuple[int, dict]:
        if not str(self.cfg.get("google_client_id") or "").strip():
            return 503, {"error": "linking is not configured"}
        info = self.verify_google(id_token)
        if info is None:
            return 401, {"error": "invalid google token"}
        sub = str(info["sub"])
        email = info.get("email") if isinstance(info.get("email"), str) else None

        claimed = (self.store.tenant(tenant) or {}).get("google_sub")
        if claimed and claimed != sub:
            # A device belongs to one account. Without this a second sign-in
            # would silently hand the first user's journal to another account.
            return 409, {"error": "this device is already linked to another account"}

        existing = self.store.tenant_by_google(sub)
        merged = 0
        if existing is None:
            self.store.claim_google(tenant, sub, email)
            target = tenant
        else:
            target = existing["id"]
            if target != tenant:
                # The phone was used anonymously first; its journal joins the
                # account instead of being thrown away.
                merged = self.store.merge_tenant(tenant, target)
        key = self.store.issue_key(target, label="linked")
        print(f"linked tenant {tenant} -> {target} (merged {merged})", flush=True)
        return 200, {"tenant": target, "key": key, "merged": merged, "email": email}


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    app: App  # injected by `make_handler`

    # -------------------------------------------------------------- plumbing

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003 - stdlib signature
        print(f"{self.address_string()} {fmt % args}", flush=True)

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status: int, payload: dict) -> None:
        self._send(status, _json_bytes(payload), "application/json; charset=utf-8")

    def _client_ip(self) -> str:
        forwarded = self.headers.get("CF-Connecting-IP") or self.headers.get(
            "X-Forwarded-For"
        )
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0] if self.client_address else "unknown"

    def _tenant(self) -> str | None:
        header = self.headers.get("Authorization", "")
        token = header[7:].strip() if header.startswith("Bearer ") else ""
        if not token:
            return None
        return self.app.store.tenant_for_key(token)

    def _body(self) -> dict:
        raw = self._raw()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _raw(self) -> bytes:
        """Read the whole body, whatever the caller decides to do with it.

        A response sent while the body is still in the socket desynchronises
        the connection: cloudflared pools origin connections, so the next
        request arrives with the old body glued to its request line and the
        server answers 501 Unsupported method. Auth and path checks therefore
        run *after* this, never before.
        """
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return b""
        if length > MAX_BODY:
            # Refuse without draining: reading gigabytes to stay in sync is
            # worse than dropping the connection, which the pool discards.
            self.close_connection = True
            return b""
        return self.rfile.read(length)

    # ----------------------------------------------------------------- routes

    def do_GET(self) -> None:  # noqa: N802 - stdlib signature
        path = self.path.split("?")[0]

        if path.startswith("/a/"):
            fire_id = path[3:].strip()
            if not fire_id or len(fire_id) > 120:
                self._send(400, b"bad id", "text/plain; charset=utf-8")
                return
            target = f"fxx://alert/{fire_id}"
            html = (
                "<!doctype html><meta charset=utf-8>"
                "<meta name=viewport content='width=device-width,initial-scale=1'>"
                "<title>fxx</title>"
                "<body style='font:16px system-ui;background:#111;color:#eee;"
                "display:grid;place-items:center;height:100vh;margin:0'>"
                "<div style='text-align:center'>"
                f"<p>فتح التطبيق…</p><p><a style='color:#7cc' href='{target}'>اضغط هنا إذا لم يفتح</a></p>"
                f"<script>location.replace({json.dumps(target)})</script>"
                "</div>"
            )
            self._send(200, html.encode("utf-8"), "text/html; charset=utf-8")
            return

        tenant = self._tenant()
        if tenant is None:
            self._json(401, {"error": "unauthorised"})
            return

        if path == "/health":
            self._json(200, self.app.health(tenant))
            return

        if path == "/events":
            self._events(tenant)
            return

        if path == "/":
            health = self.app.health(tenant)
            text = (
                f"fxx relay v{health['version']}\n"
                f"uptime {health['uptimeSec']}s\n"
                f"alerts {health['alerts']} (armed {health['armed']}, "
                f"server-side {health['serverSide']})\n"
                f"fires {health['fires']}\n"
                f"tenants {health['tenants']}\n"
                f"tunnel {health['tunnel']}\n"
            )
            self._send(200, text.encode("utf-8"), "text/plain; charset=utf-8")
            return

        self._json(404, {"error": "not found"})

    def _events(self, tenant: str) -> None:
        """Long-poll: answer as soon as a fire newer than `since` exists.

        A long-poll instead of a stream: it crosses Cloudflare's proxy without
        relying on chunked delivery, and a reconnect naturally replays anything
        missed while the phone had no signal, because the cursor is the last
        `_u` the client saw.

        `next` is the moment *before* the last read, never after it, so a fire
        written during the wait cannot be skipped by advancing past it.
        """
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        try:
            since = int((query.get("since") or ["0"])[0])
        except ValueError:
            since = 0
        try:
            wait = int((query.get("wait") or ["25"])[0])
        except ValueError:
            wait = 25
        wait = max(0, min(wait, EVENTS_MAX_WAIT))

        deadline = time.time() + wait
        while True:
            probe_at = int(time.time() * 1000)
            fires = self.app.store.fires_since(tenant, since)
            if fires or time.time() >= deadline:
                newest = max((int(f.get("_u") or 0) for f in fires), default=0)
                self._json(
                    200,
                    {
                        "now": int(time.time() * 1000),
                        "fires": fires,
                        "next": newest if fires else probe_at,
                    },
                )
                return
            time.sleep(EVENTS_POLL_SECONDS)

    def do_POST(self) -> None:  # noqa: N802 - stdlib signature
        path = self.path.split("?")[0]
        payload = self._body()  # always first: keeps the connection in sync

        if path == "/register":
            status, body = self.app.register(self._client_ip())
            self._json(status, body)
            return

        tenant = self._tenant()
        if tenant is None:
            self._json(401, {"error": "unauthorised"})
            return

        if path == "/link":
            id_token = payload.get("id_token")
            if not isinstance(id_token, str) or not id_token:
                self._json(400, {"error": "id_token required"})
                return
            status, body = self.app.link(tenant, id_token)
            self._json(status, body)
            return

        if path != "/sync":
            self._json(404, {"error": "not found"})
            return

        device = payload.get("device")
        device = device if isinstance(device, str) and device else "unknown"
        since = payload.get("since")
        since = int(since) if isinstance(since, (int, float)) else 0
        push = payload.get("push")
        push = push if isinstance(push, dict) else {}

        try:
            changed = self.app.store.apply_push(tenant, push, device)
        except Exception as error:  # a malformed record must not kill the server
            self.app.note_error(f"sync push: {error}")
            self._json(400, {"error": "bad record", "detail": str(error)})
            return

        now = int(time.time() * 1000)
        pull = self.app.store.pull(tenant, since)
        self.app.last_sync_ms = now
        self._json(
            200,
            {
                "now": now,
                "changed": changed,
                "pull": pull,
                "server": {
                    "version": VERSION,
                    "lastTick": self.app.last_tick_ms,
                    "tunnel": self.app.tunnel_url(),
                },
            },
        )


def make_handler(app: App) -> type[Handler]:
    return type("BoundHandler", (Handler,), {"app": app})


def serve(app: App, host: str, port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(app))
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, name="http", daemon=True)
    thread.start()
    return server
