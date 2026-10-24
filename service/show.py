import json
import os

config = json.load(open(os.path.expanduser("~/fxx-relay/config.json"), encoding="utf-8"))
print("KEY=" + config["key"])
print("TOPIC=" + config["ntfy_topic"])
print("PORT=" + str(config["port"]))
