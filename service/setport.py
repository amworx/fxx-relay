"""Pick a bindable port and write it to config.json (one-off, idempotent).

Port 5060 turned out to be taken by something Android owns but does not show to
an unprivileged `netstat` - SIP/IMS. Any free port above 1024 is fine.
"""

import json
import os
import socket

PATH = os.path.expanduser("~/fxx-relay/config.json")

config = json.load(open(PATH, encoding="utf-8"))
current = int(config.get("port") or 0)

probe = socket.socket()
try:
    probe.bind(("127.0.0.1", current))
    print(f"port {current} is free")
    probe.close()
    raise SystemExit(0)
except OSError:
    probe.close()

for candidate in (8787, 8788, 8789, 8099, 9099, 7070):
    probe = socket.socket()
    try:
        probe.bind(("127.0.0.1", candidate))
    except OSError:
        probe.close()
        continue
    probe.close()
    config["port"] = candidate
    with open(PATH, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)
    os.chmod(PATH, 0o600)
    print(f"port {current} busy -> using {candidate}")
    raise SystemExit(0)

raise SystemExit("no free port found")
