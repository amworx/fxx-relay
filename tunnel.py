"""Keeps a Cloudflare quick tunnel alive and republishes its URL.

A quick tunnel needs no account and no domain, but its hostname changes every
time the process starts - and this phone reboots fairly often. So the URL is
written to `data/url.txt` (for `/health` and the click link) and published to the
ntfy topic the app derives from the sync key, which is how a device rediscovers
the relay after a reboot without any manual step.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys

import wire

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
URL_PATTERN = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
CLOUDFLARED = shutil.which("cloudflared") or "/data/data/com.termux/files/usr/bin/cloudflared"


def main() -> int:
    with open(os.path.join(BASE, "config.json"), encoding="utf-8") as handle:
        config = json.load(handle)

    os.makedirs(DATA, exist_ok=True)
    port = config["port"]
    process = subprocess.Popen(
        [CLOUDFLARED, "tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    announced = False
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        if announced:
            continue
        match = URL_PATTERN.search(line)
        if not match:
            continue
        url = match.group(0)
        with open(os.path.join(DATA, "url.txt"), "w", encoding="utf-8") as handle:
            handle.write(url + "\n")
        sent = wire.publish(
            config["ntfy_base"],
            config["ntfy_topic"] + "-url",
            "fxx relay address",
            url,
            priority=2,
            tags=["link"],
        )
        print(f"tunnel url {url} (published={sent})", flush=True)
        announced = True

    return process.wait()


if __name__ == "__main__":
    sys.exit(main())
