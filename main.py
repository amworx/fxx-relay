"""fxx relay - entry point.

Runs under runit: one process, two threads (HTTP + evaluator). No dependencies
beyond the Python standard library, because the host is Termux and `pkg` is not
to be touched.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time

import evaluator
import wire
from api import App, serve
from store import Store

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _state_dir() -> str:
    """Where config and a local database may be written.

    The phone relay owns its directory; a container host mounts the image
    read-only and wipes /tmp between deploys, which is why production there
    keeps its records in Postgres and only the config is local.
    """
    if os.access(BASE_DIR, os.W_OK):
        return BASE_DIR
    return "/tmp"


STATE_DIR = _state_dir()
DATA_DIR = os.path.join(STATE_DIR, "data")
CONFIG_PATH = os.path.join(STATE_DIR, "config.json")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

DEFAULTS = {
    "host": "127.0.0.1",
    "port": 5060,
    "symbol": "XAUUSD",
    "ntfy_base": "https://ntfy.sh",
    "ntfy_enabled": False,
    "tick_seconds": 20,
    "bars_limit": 120,
}

# Environment name -> config key. The host sets these; the file is a fallback
# for the phone relay, where an operator edits config.json directly.
ENV_SETTINGS = (
    ("GOOGLE_CLIENT_ID", "google_client_id"),
    ("PUBLIC_URL", "public_url"),
    ("SYMBOL", "symbol"),
    ("TICK_SECONDS", "tick_seconds"),
)


def topic_for(key: str) -> str:
    """Deterministic, non-reversible topic name derived from the sync key.

    The app can compute the same value, which is what lets it rediscover the
    tunnel URL after a reboot without any extra pairing field.
    """
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return "fxx-" + digest[:16]


def load_config() -> dict:
    config: dict = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as handle:
            config = json.load(handle)

    changed = False
    if not config.get("key"):
        # 160 bits of entropy, base32 so it survives copy/paste and typing.
        config["key"] = (
            secrets.token_bytes(20)
            .hex()
            .upper()
        )
        changed = True
    for name, value in DEFAULTS.items():
        if name not in config:
            config[name] = value
            changed = True
    # Settings the host injects. On a container host config.json is recreated
    # empty on every deploy, so anything that lives there has to come from the
    # environment or it is silently absent (Google linking was exactly that).
    for env_name, name in ENV_SETTINGS:
        value = os.environ.get(env_name)
        if value and config.get(name) != value:
            config[name] = value
            changed = True
    config["ntfy_topic"] = topic_for(config["key"])

    if changed:
        with open(CONFIG_PATH, "w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=2)
        os.chmod(CONFIG_PATH, 0o600)
    return config


def announce_address(config: dict, data_dir: str) -> None:
    """Publish the relay's public address, which is now a fixed hostname.

    With a named tunnel the address no longer changes, so this is written once
    at startup instead of being scraped from cloudflared's output. The ntfy
    publish stays as the recovery path for a device whose stored URL is stale -
    it is not how a paired device learns the address any more.
    """
    url = config.get("public_url")
    if not url:
        return
    with open(os.path.join(data_dir, "url.txt"), "w", encoding="utf-8") as handle:
        handle.write(url + "\n")
    if not config.get("ntfy_enabled"):
        print(f"address {url}", flush=True)
        return
    sent = wire.publish(
        config["ntfy_base"],
        config["ntfy_topic"] + "-url",
        "fxx relay address",
        url,
        priority=2,
        tags=["link"],
    )
    print(f"address {url} (published={sent})", flush=True)


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    config = load_config()
    # Hostless injects PORT; respect it, but fall back to config for local runs.
    config["port"] = int(os.environ.get("PORT", config["port"]))
    store = Store(DATABASE_URL or os.path.join(DATA_DIR, "fxx.db"))
    app = App(store, config, DATA_DIR)

    announce_address(config, DATA_DIR)

    # A container host publishes one fixed address; bind where it can reach us.
    host = "0.0.0.0" if DATABASE_URL or not os.access(BASE_DIR, os.W_OK) else config["host"]
    serve(app, host, int(config["port"]))
    print(
        f"fxx relay listening on {host}:{config['port']} "
        f"symbol={config['symbol']} tick={config['tick_seconds']}s "
        f"tenants={store.tenant_count()} "
        f"linking={'on' if config.get('google_client_id') else 'off'}",
        flush=True,
    )

    interval = max(5, int(config.get("tick_seconds", 20)))
    while True:
        started = time.time()
        try:
            evaluator.tick(app)
        except Exception as error:  # keep the loop alive; report on /health
            app.note_error(f"tick: {type(error).__name__}: {error}")
            print(f"tick failed: {error}", flush=True)
        elapsed = time.time() - started
        time.sleep(max(1.0, interval - elapsed))


if __name__ == "__main__":
    main()
