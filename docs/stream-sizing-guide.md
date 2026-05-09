# Stream Sizing Guide

Use retention to prevent unbounded stream growth:

- Per-write: `XADD key MAXLEN [~|=] count ...`
- Post-hoc trim: `XTRIM key MAXLEN [~|=] count`
- Time/ID window trim: `XTRIM key MINID [~|=] threshold-id`
- Server default for `XADD` without `MAXLEN`:
  - `SWARM_KEYDB_STREAM_DEFAULT_MAXLEN`
  - `SWARM_KEYDB_STREAM_DEFAULT_MAXLEN_APPROXIMATE`

Approximate (`~`) trimming favors lower trim frequency and keeps stream size bounded close to target (within roughly 10%).

## Quick sizing examples

- IoT telemetry (1k msg/s, 24h replay): set `MAXLEN ~ 86_400_000`
- Chat (100 msg/s, 7d replay): set `MAXLEN ~ 60_480_000`
- Audit/event sourcing (10 msg/s, 30d replay): set `MAXLEN ~ 25_920_000`

Monitor:

- `swarmkeydb_stream_trimmed_total` to verify trims are happening
- `swarmkeydb_stream_length_bytes` to track memory footprint (total + per stream)
