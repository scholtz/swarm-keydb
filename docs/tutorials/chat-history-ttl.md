# Tutorial: Chat Message History with TTL

Goal: store chat messages that expire automatically.

## Run

```bash
dotnet run --project src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
redis-cli -p 6379 SETEX chat:room1:msg:1 60 "hello"
redis-cli -p 6379 SETEX chat:room1:msg:2 60 "how are you?"
redis-cli -p 6379 TTL chat:room1:msg:1
redis-cli -p 6379 GET chat:room1:msg:1
```

## Notes

- Use `SETEX` for seconds or `PSETEX` for milliseconds.
- TTL must be positive (`ERR invalid expire seconds` otherwise).
