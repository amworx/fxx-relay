"""Persistence: tenants, the sync store, the evaluation cursor, alert memory.

Two dialects, one code path. A local path gives SQLite (tests, the phone relay);
a `postgres://` URL gives Postgres (the cloud relay, where the container
filesystem is wiped on every deploy). The SQL is written once and the wrapper
rewrites placeholders, so a statement cannot drift between the two.

One connection, one lock. The workload is a handful of statements per minute, so
serialising access costs nothing and removes a whole class of races between the
HTTP thread and the evaluator thread.

**Every record belongs to a tenant.** There is no default tenant on the record
methods on purpose: a call site that forgets the tenant is a compile-time error
in a typed language and a silent cross-user leak in this one, so the signature
forces the decision at every call.

Millisecond columns are BIGINT everywhere: epoch milliseconds overflow a 32-bit
integer, which SQLite would silently accept and Postgres would reject.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time

BUCKETS = ("alerts", "fires", "journal", "settings", "profile")

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
  id         TEXT PRIMARY KEY,
  created    BIGINT NOT NULL,
  google_sub TEXT UNIQUE,
  email      TEXT,
  label      TEXT
);

CREATE TABLE IF NOT EXISTS tenant_keys (
  key_hash TEXT PRIMARY KEY,
  tenant   TEXT NOT NULL,
  created  BIGINT NOT NULL,
  label    TEXT
);
CREATE INDEX IF NOT EXISTS tenant_keys_tenant ON tenant_keys (tenant);

CREATE TABLE IF NOT EXISTS kv_sync (
  tenant     TEXT    NOT NULL,
  bucket     TEXT    NOT NULL,
  id         TEXT    NOT NULL,
  updated_at BIGINT  NOT NULL,
  device     TEXT    NOT NULL,
  deleted    INTEGER NOT NULL,
  body       TEXT    NOT NULL,
  PRIMARY KEY (tenant, bucket, id)
);
CREATE INDEX IF NOT EXISTS kv_sync_pull ON kv_sync (tenant, bucket, updated_at);

CREATE TABLE IF NOT EXISTS cursors (
  interval TEXT PRIMARY KEY,
  bar_ms   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS server_alerts (
  tenant         TEXT   NOT NULL,
  id             TEXT   NOT NULL,
  armed          INTEGER NOT NULL,
  watching_since BIGINT NOT NULL,
  last_fired_ms  BIGINT,
  last_bar_ms    BIGINT,
  PRIMARY KEY (tenant, id)
);

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
"""


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _new_key() -> str:
    # 192 bits, hex so it survives copy/paste and never needs escaping.
    return secrets.token_hex(24)


def _new_tenant_id() -> str:
    return "t_" + secrets.token_hex(8)


def _is_dsn(target: str) -> bool:
    return target.startswith(("postgres://", "postgresql://"))


def _is_disconnect(error: Exception) -> bool:
    """A dropped cloud connection, as opposed to a bad statement."""
    return type(error).__name__ in ("OperationalError", "InterfaceError")


class _Cursor:
    """The slice of the cursor API the store uses, over a psycopg cursor."""

    def __init__(self, cursor) -> None:
        self._cursor = cursor

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount


class _Db:
    """One connection, dialect-normalised.

    Rows come back as mappings in both dialects (`sqlite3.Row` and `dict_row`
    both answer `row["name"]` and `dict(row)`), so call sites need no branch.
    A lost Postgres connection is reopened once before the statement is retried,
    because the relay is expected to outlive a database restart.
    """

    def __init__(self, target: str) -> None:
        self.is_pg = _is_dsn(target)
        self.target = target
        self._db = None
        self._connect()

    def _connect(self) -> None:
        if self.is_pg:
            import psycopg
            from psycopg.rows import dict_row

            self._db = psycopg.connect(
                self.target, autocommit=True, row_factory=dict_row
            )
            return
        os.makedirs(os.path.dirname(self.target) or ".", exist_ok=True)
        self._db = sqlite3.connect(
            self.target, check_same_thread=False, isolation_level=None
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")

    def _sql(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.is_pg else sql

    def execute(self, sql: str, params=()):
        try:
            return self._run(sql, params)
        except Exception as error:
            if not self.is_pg or not _is_disconnect(error):
                raise
            self._connect()
            return self._run(sql, params)

    def _run(self, sql: str, params):
        text = self._sql(sql)
        if self.is_pg:
            cursor = self._db.cursor()
            cursor.execute(text, params)
            return _Cursor(cursor)
        return self._db.execute(text, params)

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)

    @contextlib.contextmanager
    def tx(self):
        """A transaction in whichever dialect, so a rollback never leaks."""
        if self.is_pg:
            with self._db.transaction():
                yield
            return
        self._db.execute("BEGIN")
        try:
            yield
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self._db.close()


class Store:
    def __init__(self, target: str) -> None:
        """`target` is a SQLite path or a Postgres DSN."""
        self.target = target
        self.path = "" if _is_dsn(target) else target
        self._lock = threading.RLock()
        self._db = _Db(target)
        with self._lock:
            self._migrate_legacy()
            self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    @property
    def is_durable(self) -> bool:
        """True when records outlive the process, which on a container host is
        the difference between a backup and a scratch file."""
        return self._db.is_pg

    # ------------------------------------------------------------- migration

    def _columns(self, table: str) -> set[str]:
        if self._db.is_pg:
            rows = self._db.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name=? AND table_schema=current_schema()",
                (table,),
            ).fetchall()
            return {row["column_name"] for row in rows}
        rows = self._db.execute(f"PRAGMA table_info({table})").fetchall()
        return {row["name"] for row in rows}

    def _migrate_legacy(self) -> None:
        """Single-tenant databases (pre-v33) become tenant `main`.

        Their records keep their timestamps, so nothing about the existing
        device's history changes - it simply becomes one tenant among many.
        Each table is checked on its own, because a database can be left
        half-migrated by a cut that rebuilt only one of them.

        Postgres never carries a pre-v33 database: the cloud relay starts empty.
        """
        if self._db.is_pg:
            return
        self._migrate_kv_sync()
        self._migrate_server_alerts()

    def _table_exists(self, table: str) -> bool:
        if self._db.is_pg:
            row = self._db.execute(
                "SELECT 1 FROM information_schema.tables"
                " WHERE table_name=? AND table_schema=current_schema()",
                (table,),
            ).fetchone()
            return row is not None
        return bool(
            self._db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
        )

    def _migrate_kv_sync(self) -> None:
        if not self._table_exists("kv_sync") or "tenant" in self._columns("kv_sync"):
            return
        with self._db.tx():
            self._db.execute(
                "CREATE TABLE kv_sync_v33 ("
                " tenant TEXT NOT NULL, bucket TEXT NOT NULL, id TEXT NOT NULL,"
                " updated_at BIGINT NOT NULL, device TEXT NOT NULL,"
                " deleted INTEGER NOT NULL, body TEXT NOT NULL,"
                " PRIMARY KEY (tenant, bucket, id))"
            )
            self._db.execute(
                "INSERT INTO kv_sync_v33 (tenant, bucket, id, updated_at, device, deleted, body)"
                " SELECT 'main', bucket, id, updated_at, device, deleted, body FROM kv_sync"
            )
            self._db.execute("DROP TABLE kv_sync")
            self._db.execute("ALTER TABLE kv_sync_v33 RENAME TO kv_sync")
        print("migrated kv_sync to per-tenant rows (tenant 'main')", flush=True)

    def _migrate_server_alerts(self) -> None:
        if not self._table_exists("server_alerts") or "tenant" in self._columns(
            "server_alerts"
        ):
            return
        with self._db.tx():
            self._db.execute(
                "CREATE TABLE server_alerts_v33 ("
                " tenant TEXT NOT NULL, id TEXT NOT NULL, armed INTEGER NOT NULL,"
                " watching_since BIGINT NOT NULL, last_fired_ms BIGINT,"
                " last_bar_ms BIGINT, PRIMARY KEY (tenant, id))"
            )
            self._db.execute(
                "INSERT INTO server_alerts_v33"
                " (tenant, id, armed, watching_since, last_fired_ms, last_bar_ms)"
                " SELECT 'main', id, armed, watching_since, last_fired_ms, last_bar_ms"
                " FROM server_alerts"
            )
            self._db.execute("DROP TABLE server_alerts")
            self._db.execute("ALTER TABLE server_alerts_v33 RENAME TO server_alerts")
        print("migrated server_alerts to per-tenant rows (tenant 'main')", flush=True)

    # --------------------------------------------------------------- tenants

    def create_tenant(
        self,
        google_sub: str | None = None,
        email: str | None = None,
        tenant_id: str | None = None,
    ) -> tuple[str, str]:
        """A new tenant and its first key. The key is returned once, never stored."""
        tid = tenant_id or _new_tenant_id()
        key = _new_key()
        now = int(time.time())
        with self._lock:
            self._db.execute(
                "INSERT INTO tenants (id, created, google_sub, email, label)"
                " VALUES (?,?,?,?,NULL)",
                (tid, now, google_sub, email),
            )
            self._db.execute(
                "INSERT INTO tenant_keys (key_hash, tenant, created, label)"
                " VALUES (?,?,?,NULL)",
                (_hash_key(key), tid, now),
            )
        return tid, key

    def ensure_legacy_key(self, key: str) -> None:
        """Keep the pre-v33 single key working as tenant `main`.

        Installed devices still carry that key; without this they would start
        getting 401 the moment the relay is upgraded, and their un-synced alerts
        would sit on the phone until the next app release.
        """
        if not key:
            return
        with self._lock:
            row = self._db.execute(
                "SELECT tenant FROM tenant_keys WHERE key_hash=?", (_hash_key(key),)
            ).fetchone()
            if row is not None:
                return
            row = self._db.execute("SELECT 1 FROM tenants WHERE id='main'").fetchone()
            if row is None:
                self._db.execute(
                    "INSERT INTO tenants (id, created, google_sub, email, label)"
                    " VALUES ('main',?,NULL,NULL,'legacy')",
                    (int(time.time()),),
                )
            self._db.execute(
                "INSERT INTO tenant_keys (key_hash, tenant, created, label)"
                " VALUES (?, 'main', ?, 'legacy')",
                (_hash_key(key), int(time.time())),
            )

    def tenant_for_key(self, key: str) -> str | None:
        if not key:
            return None
        with self._lock:
            row = self._db.execute(
                "SELECT tenant FROM tenant_keys WHERE key_hash=?", (_hash_key(key),)
            ).fetchone()
        return row["tenant"] if row else None

    def issue_key(self, tenant: str, label: str | None = None) -> str:
        """Another key for an existing tenant. Old keys stay valid, so a second
        device signing in does not knock the first one off."""
        key = _new_key()
        with self._lock:
            self._db.execute(
                "INSERT INTO tenant_keys (key_hash, tenant, created, label) VALUES (?,?,?,?)",
                (_hash_key(key), tenant, int(time.time()), label),
            )
        return key

    def revoke_key(self, key: str) -> bool:
        with self._lock:
            cur = self._db.execute(
                "DELETE FROM tenant_keys WHERE key_hash=?", (_hash_key(key),)
            )
        return cur.rowcount > 0

    def tenant(self, tenant: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT id, created, google_sub, email, label FROM tenants WHERE id=?",
                (tenant,),
            ).fetchone()
        return dict(row) if row else None

    def tenant_by_google(self, sub: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT id, created, google_sub, email, label FROM tenants WHERE google_sub=?",
                (sub,),
            ).fetchone()
        return dict(row) if row else None

    def claim_google(self, tenant: str, sub: str, email: str | None) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE tenants SET google_sub=?, email=? WHERE id=?", (sub, email, tenant)
            )

    def tenant_count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) c FROM tenants").fetchone()["c"]

    def tenants_with_alerts(self) -> list[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT DISTINCT tenant FROM kv_sync WHERE bucket='alerts' AND deleted=0"
            ).fetchall()
        return [row["tenant"] for row in rows]

    def merge_tenant(self, src: str, dst: str) -> int:
        """Copy every record of `src` into `dst` under the same last-write-wins
        rule, so signing in on a phone that already had an anonymous journal
        cannot clobber the account's newer records."""
        if src == dst:
            return 0
        moved = 0
        with self._lock:
            rows = self._db.execute(
                "SELECT bucket, id, updated_at, device, deleted, body FROM kv_sync WHERE tenant=?",
                (src,),
            ).fetchall()
            with self._db.tx():
                for row in rows:
                    record = json.loads(row["body"])
                    record["id"] = row["id"]
                    record["_u"] = row["updated_at"]
                    record["_d"] = bool(row["deleted"])
                    if self.put(dst, row["bucket"], record, row["device"]):
                        moved += 1
        return moved

    # ---------------------------------------------------------------- sync

    def put(self, tenant: str, bucket: str, record: dict, device: str) -> bool:
        """Upsert one record. True when it changed the stored row."""
        if bucket not in BUCKETS:
            raise ValueError(f"unknown bucket {bucket!r}")
        rid = record.get("id")
        if not isinstance(rid, str) or not rid:
            raise ValueError("record without id")
        updated = int(record.get("_u") or 0)
        deleted = 1 if record.get("_d") else 0
        body = json.dumps(
            {k: v for k, v in record.items() if k not in ("_u", "_d")},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with self._lock:
            row = self._db.execute(
                "SELECT updated_at, device FROM kv_sync WHERE tenant=? AND bucket=? AND id=?",
                (tenant, bucket, rid),
            ).fetchone()
            if row is not None:
                if updated < row["updated_at"]:
                    return False
                if updated == row["updated_at"] and device <= row["device"]:
                    return False
            self._db.execute(
                "INSERT INTO kv_sync (tenant, bucket, id, updated_at, device, deleted, body)"
                " VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(tenant, bucket, id) DO UPDATE SET"
                "   updated_at=excluded.updated_at, device=excluded.device,"
                "   deleted=excluded.deleted, body=excluded.body",
                (tenant, bucket, rid, updated, device, deleted, body),
            )
            return True

    def exists(self, tenant: str, bucket: str, rid: str) -> bool:
        with self._lock:
            row = self._db.execute(
                "SELECT 1 FROM kv_sync WHERE tenant=? AND bucket=? AND id=?",
                (tenant, bucket, rid),
            ).fetchone()
            return row is not None

    def pull(self, tenant: str, since: int, buckets=BUCKETS) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        with self._lock:
            for bucket in buckets:
                rows = self._db.execute(
                    "SELECT id, updated_at, deleted, body FROM kv_sync"
                    " WHERE tenant=? AND bucket=? AND updated_at>? ORDER BY updated_at, id",
                    (tenant, bucket, since),
                ).fetchall()
                records = []
                for row in rows:
                    record = json.loads(row["body"])
                    record["id"] = row["id"]
                    record["_u"] = row["updated_at"]
                    record["_d"] = bool(row["deleted"])
                    records.append(record)
                out[bucket] = records
        return out

    def fires_since(self, tenant: str, since: int, limit: int = 20) -> list[dict]:
        """Fire records written after `since`, oldest first.

        This is the wake-up path for a phone that holds a long-poll open, so it
        is deliberately the same shape as `pull` and bounded: a client that has
        been away for a week gets the oldest few and advances its cursor rather
        than a thousand rows it would only discard.
        """
        with self._lock:
            rows = self._db.execute(
                "SELECT id, updated_at, deleted, body FROM kv_sync"
                " WHERE tenant=? AND bucket='fires' AND updated_at>?"
                " ORDER BY updated_at, id LIMIT ?",
                (tenant, since, limit),
            ).fetchall()
            out = []
            for row in rows:
                record = json.loads(row["body"])
                record["id"] = row["id"]
                record["_u"] = row["updated_at"]
                record["_d"] = bool(row["deleted"])
                out.append(record)
            return out

    def apply_push(self, tenant: str, push: dict, device: str) -> int:
        changed = 0
        with self._lock:
            with self._db.tx():
                for bucket, records in (push or {}).items():
                    if bucket not in BUCKETS or not isinstance(records, list):
                        continue
                    for record in records:
                        if isinstance(record, dict) and self.put(
                            tenant, bucket, record, device
                        ):
                            changed += 1
        return changed

    # ------------------------------------------------------------- alerts

    def alerts(self, tenant: str) -> list[dict]:
        return [r for r in self.pull(tenant, 0, ("alerts",))["alerts"] if not r.get("_d")]

    def counts(self, tenant: str) -> dict:
        with self._lock:
            total = self._db.execute(
                "SELECT COUNT(*) c FROM kv_sync WHERE tenant=? AND bucket='alerts' AND deleted=0",
                (tenant,),
            ).fetchone()["c"]
            fires = self._db.execute(
                "SELECT COUNT(*) c FROM kv_sync WHERE tenant=? AND bucket='fires'",
                (tenant,),
            ).fetchone()["c"]
            journal = self._db.execute(
                "SELECT COUNT(*) c FROM kv_sync WHERE tenant=? AND bucket='journal' AND deleted=0",
                (tenant,),
            ).fetchone()["c"]
        return {"alerts": total, "fires": fires, "journal": journal}

    def totals(self) -> dict:
        """Whole-relay numbers, for an operator looking at /health."""
        with self._lock:
            alerts = self._db.execute(
                "SELECT COUNT(*) c FROM kv_sync WHERE bucket='alerts' AND deleted=0"
            ).fetchone()["c"]
            fires = self._db.execute(
                "SELECT COUNT(*) c FROM kv_sync WHERE bucket='fires'"
            ).fetchone()["c"]
        return {"alerts": alerts, "fires": fires}

    # ------------------------------------------------------------ cursors

    def cursor(self, interval: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT bar_ms FROM cursors WHERE interval=?", (interval,)
            ).fetchone()
            return row["bar_ms"] if row else 0

    def set_cursor(self, interval: str, bar_ms: int) -> None:
        # The max is taken here rather than in SQL: `MAX(a, b)` is a scalar in
        # SQLite and an aggregate in Postgres, so one expression cannot serve
        # both. The lock is already held, so the read and write are atomic.
        with self._lock:
            row = self._db.execute(
                "SELECT bar_ms FROM cursors WHERE interval=?", (interval,)
            ).fetchone()
            value = bar_ms if row is None else max(int(row["bar_ms"]), int(bar_ms))
            self._db.execute(
                "INSERT INTO cursors (interval, bar_ms) VALUES (?,?)"
                " ON CONFLICT(interval) DO UPDATE SET bar_ms=excluded.bar_ms",
                (interval, value),
            )

    def cursors(self) -> dict[str, int]:
        with self._lock:
            rows = self._db.execute("SELECT interval, bar_ms FROM cursors").fetchall()
        return {row["interval"]: row["bar_ms"] for row in rows}

    # ------------------------------------------------------- alert memory

    def alert_state(self, tenant: str, alert_id: str) -> dict:
        with self._lock:
            row = self._db.execute(
                "SELECT armed, watching_since, last_fired_ms, last_bar_ms"
                " FROM server_alerts WHERE tenant=? AND id=?",
                (tenant, alert_id),
            ).fetchone()
        if row is None:
            return {"armed": None, "watching_since": 0, "last_fired_ms": None, "last_bar_ms": None}
        return dict(row)

    def remember_alert(self, tenant: str, alert_id: str, armed: bool, watching_ms: int) -> None:
        """Keep the alert's arm state so re-arming can be told from a stale row."""
        armed = 1 if armed else 0
        watching = int(watching_ms or 0)
        with self._lock:
            row = self._db.execute(
                "SELECT armed, watching_since FROM server_alerts WHERE tenant=? AND id=?",
                (tenant, alert_id),
            ).fetchone()
            if row is None:
                self._db.execute(
                    "INSERT INTO server_alerts"
                    " (tenant, id, armed, watching_since, last_fired_ms, last_bar_ms)"
                    " VALUES (?,?,?,?,NULL,NULL)",
                    (tenant, alert_id, armed, watching),
                )
                return
            # A re-arm, or a new condition, makes an earlier fire irrelevant.
            rearmed = row["armed"] == 0 and armed == 1
            rewatched = row["watching_since"] != watching
            if rearmed or rewatched:
                self._db.execute(
                    "UPDATE server_alerts SET armed=?, watching_since=?, last_fired_ms=NULL"
                    " WHERE tenant=? AND id=?",
                    (armed, watching, tenant, alert_id),
                )
            else:
                self._db.execute(
                    "UPDATE server_alerts SET armed=?, watching_since=? WHERE tenant=? AND id=?",
                    (armed, watching, tenant, alert_id),
                )

    def mark_fired(self, tenant: str, alert_id: str, fired_at_ms: int) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE server_alerts SET last_fired_ms=?, last_bar_ms=?"
                " WHERE tenant=? AND id=?",
                (fired_at_ms, fired_at_ms, tenant, alert_id),
            )

    # --------------------------------------------------------------- misc

    def meta_get(self, key: str) -> str | None:
        with self._lock:
            row = self._db.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
            return row["v"] if row else None

    def meta_set(self, key: str, value: str) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO meta (k, v) VALUES (?,?)"
                " ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (key, value),
            )

    def size_kb(self) -> int:
        if self._db.is_pg:
            with self._lock:
                row = self._db.execute(
                    "SELECT pg_database_size(current_database()) AS n"
                ).fetchone()
            return int(row["n"] or 0) // 1024
        total = 0
        for suffix in ("", "-wal", "-shm"):
            try:
                total += os.path.getsize(self.path + suffix)
            except OSError:
                pass
        return total // 1024
