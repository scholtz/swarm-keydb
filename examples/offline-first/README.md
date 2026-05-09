# Offline-first example

This example demonstrates how to run SwarmKeyDb with the offline journal enabled and then recover after a Bee outage.

## Start the stack

```bash
docker compose up --build
```

## Simulate a partition

1. Stop the Bee service: `docker compose stop bee`
2. Write data through Redis while Bee is down:
   ```bash
   redis-cli -p 6379 SET offline:1 one
   redis-cli -p 6379 SET offline:2 two
   curl http://localhost:8080/health
   ```
3. Confirm `offline_queue_depth` increases.
4. Restart Bee: `docker compose start bee`
5. Wait a few seconds and confirm the queue drains back to `0`.

The server is configured with `SWARM_KEYDB_OFFLINE_MODE=auto`, `SWARM_KEYDB_OFFLINE_JOURNAL=sqlite`, and a 1-second sync interval so replay is easy to observe.
