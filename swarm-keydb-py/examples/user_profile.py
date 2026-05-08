from swarm_keydb import SwarmKeyDb

client = SwarmKeyDb(host="127.0.0.1", port=6379)
client.put("user:42", '{"name":"Ada","role":"admin"}')
try:
    print(client.get("user:42"))
except Exception as exc:
    print(f"Failed to read user:42: {exc}")
