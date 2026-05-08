from swarm_keydb import SwarmKeyDb

client = SwarmKeyDb(host="127.0.0.1", port=6379)
client.batch_put({"config:theme": "dark", "config:region": "eu-west-1"})
print(client.batch_get(["config:theme", "config:region"]))
