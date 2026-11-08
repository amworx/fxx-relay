# fxx relay — protocol v2

Server: `~/fxx-relay/` on the phone at `10.10.0.5` (Python 3.13, stdlib only).
Client: the Flutter app (`SyncService`), no new packages (`dart:io` HttpClient).

v2 (v33 of the app) made the relay **multi-tenant**: one key = one tenant = one
user, so the app can be shared. A v1 database is migrated in place on first
start (everything lands in tenant `main`, timestamps untouched).

## Auth

Every request carries `Authorization: Bearer <tenant-key>`; the key resolves to
exactly one tenant. Keys are 48 hex chars from `secrets.token_hex(24)` and only
their SHA-256 is stored (`tenant_keys`), so a database copy does not leak usable
keys. A tenant may hold several keys: signing in on a second phone issues a new
one and leaves the first working.

`config.json`'s old shared `key` keeps working as tenant `main` until it is
revoked (`store.revoke_key`), so v32 devices keep syncing across the cut.

## POST /register

No auth (the first request a fresh install makes). Mints a tenant and its first
key:

```json
{ "tenant": "t_67700b9dd672df88", "key": "832ed1ee…", "now": 1792849386995,
  "server": { "version": "2" } }
```

201 on success. Rate limited: 10 per hour per client address
(`CF-Connecting-IP` / `X-Forwarded-For`), 200 per hour overall, and the relay
stops minting at 5000 tenants. 429 when limited, 503 when the cap is hit.

## POST /link

`Authorization: Bearer <anonymous tenant key>` + `{ "id_token": "<Google ID token>" }`.

- 503 - the relay has no `google_client_id` configured.
- 401 - Google's `tokeninfo` rejected the token.
- 409 - this tenant is already linked to a *different* Google account.
- 200 - `{ "tenant", "key", "merged", "email" }`.

The token is verified against `https://oauth2.googleapis.com/tokeninfo`
(audience must equal the configured Web client ID, `email_verified` required)
and the account is identified by `sub`. First link claims the caller's tenant;
linking from a *second* anonymous tenant merges that tenant into the account's
one (last-write-wins per record) instead of orphaning its journal. The response
carries a fresh key; the client then rebaselines (empty push, pull from 0).


## Buckets

`alerts` · `fires` · `journal` · `settings` · `profile`

Every record is an object with `id` (string) plus opaque body fields, and two
envelope fields:

| field | meaning |
|---|---|
| `_u` | updated-at, epoch **ms** (client clock, monotonic per device) |
| `_d` | `true` for a tombstone (deleted) |

`settings` / `profile` use the setting name as `id` (e.g. `theme_mode`).

## POST /sync

```json
{ "device": "dev-1a2b3c",
  "since": 1761250000000,
  "push": { "alerts": [ {"id":"...","_u":1761250000000,"_d":false,"operator":"movesBy", "...":"..."} ] } }
```

```json
{ "now": 1761250001234,
  "pull": { "alerts": [ ... ], "fires": [ ... ] },
  "server": { "lastTick": 1761250000000, "version": "1" } }
```

Rules:

- **Upsert on push**: a record wins if `_u` is newer than the stored `_u`;
  on a tie the lexicographically greater `device` wins. Tombstones are stored
  like any record and returned like any record, so a delete propagates once.
- **Pull**: every record with `_u > since`. The client sends `since = now` from
  the previous response (first sync: `since = 0` → full snapshot).
- The relay never deletes tombstones automatically; pruning is manual.
- A push is applied before the pull is computed, in one transaction.

## GET /health

Tenant-scoped:

```json
{ "ok": true, "version": "2", "tenant": "t_…", "uptimeSec": 100,
  "lastTickMs": 1792849418035, "lastSyncMs": 1792849387423,
  "alerts": 1, "armed": 0, "serverSide": 0, "appOnly": 0,
  "fires": 0, "journal": 0, "cursors": { "15m": 1792787400000 },
  "dbKB": 2168, "tenants": 2, "tunnel": "https://itworx.dpdns.org",
  "lastError": null }
```

No auth bypass: health is behind the same key, because the tunnel is public.
`tenants` is the relay-wide tenant count; everything else describes the caller.
`lastError` is sticky - it keeps the last feed/push failure until it is cleared.

## GET /events?since=<ms>&wait=<sec>

The wake-up call for the phone's background service. A **long-poll**, not a
stream: the request is held open until a `fires` record newer than `since`
appears, or `wait` seconds pass (`wait` is clamped to 0..30, default 25).

```json
{ "now": 1792836874759, "fires": [ { "id": "a1@1792836870000", "_u": 1792836874719, "price": 4050.0 } ], "next": 1792836874719 }
```

- `fires` - the new records, oldest first, at most 20 per response.
- `next` - the cursor to pass as `since` on the next call. It is the newest
  `_u` returned, or - when the call timed out empty - the moment *before* the
  last read, so a record written during the wait can never be skipped.
- A timeout with `"fires": []` is a normal, healthy response: reconnect.
- Auth is the same Bearer key. No server-side subscription state exists, so a
  phone that was offline simply reconnects with its last `next` and receives
  everything it missed.

Measured end to end through the Cloudflare tunnel: ~250 ms from write to
response.

## GET /a/<fire-id>

Tiny HTML page: `fxx://alert/<fire-id>` redirect plus a fallback link. Public on
purpose: a notification cannot carry a header. This was the ntfy `click` target,
because Telegram and some launchers refuse custom schemes; ntfy publishing is now
**off by default** (`ntfy_enabled`), since a public topic would leak every user's
fires.

## Server-side evaluation

Both cadences are evaluated server-side. `barClose` reads a finished candle;
`live` reads one quote per tick and is judged against the previous sample, so a
crossing that happens while the app is off screen still produces a fire the
Android service can post. `/health` reports `serverSide` (armed alerts the loop
can judge) and `appOnly` (armed alerts it cannot, e.g. a live `movesBy`, which
`isValid` refuses by construction).

Per tick (20 s), for every tenant that has at least one alert record (armed or
not - the tick also remembers the arm state), per interval that has at least one
armed bar-close alert:

1. fetch closed bars (`isOpen == false`), ascending;
2. for every bar with `open_ms > cursor[interval]`, evaluate every alert whose
   `interval` matches, in bar order;
3. a fire is `fires` id `f<alertId>@<closes_at_ms>` — the close instant, not the bar's open, so the app derives the same id for the same candle and a bar both sides saw is one record; the primary key makes a
   duplicate physically impossible, across restarts and double ticks;
4. push once per inserted row; advance the cursor.

### Live cadence

For every armed `live` alert, once per tick:

1. stop if the gold calendar says no session is running (`gold_market_is_open`,
   New York DST rule: Sunday 18:00 ET → Friday 17:00 ET, minus the 17:00–18:00
   ET daily break). A weekend feed that republishes Friday's close must never
   look live;
2. fetch one quote (`biquote.io/api/<symbol>`, `mid` first) and drop it when its
   own clock is older than 90 s; a quote with no provider clock is timed from
   the fetch instant;
3. judge `rules.should_fire_live(alert, price, last_price, now)`: the same
   operator test as bar-close, with `last_price` as the earlier sample. A
   crossing with no previous sample is silent — the first tick only sets the
   baseline;
4. a fire is `fires` id `f<alertId>@<now_ms>`, carrying `level`, `level2`,
   `lookback` and `operator` as well as the price and the ready-made Arabic
   `message`;
5. `server_alerts.last_price` is written on **every** tick (firing or not), so
   the next tick has a baseline. Re-arming or changing `watching_since` clears
   it.

Alert-level suppression (a threshold must not ring every tick):

- `server_alerts.last_fired_ms` + `armed` + `watching_since` are remembered;
- non-crossing alerts fire at most once per `watching_since`;
- re-arming (armed `false → true`) or a new `watching_since` clears the memory;
- crossing alerts fire on every qualifying sample.

The app also evaluates live alerts while it is on screen. Its fire is pushed
within a few seconds, so the relay skips a fire when the same alert already has
one in the last 15 s; that is shorter than a tick, so a genuinely later crossing
still rings.

Gate copied from `shouldFireOnClose`: `closed_at > watchingSince`,
`latest(triggeredAt, lastFire) < closed_at`, expiry at `now`, percent needs
`lookback + 1` closed bars, `|move| + 1e-9 >= threshold`.
