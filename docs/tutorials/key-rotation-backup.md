# Tutorial: Key Rotation & Backup

Goal: rotate an Ethereum private key without losing data, create an immutable Swarm backup, and restore it into a fresh SwarmKeyDb instance.

## 1. Create two Ethereum private key files

```bash
printf '%s\n' <old-64-char-hex-private-key> > /tmp/old.key
printf '%s\n' <new-64-char-hex-private-key> > /tmp/new.key
```

## 2. Rotate the database key

```bash
skdb rotate-key --old-key /tmp/old.key --new-key /tmp/new.key
```

The command reads every key with the old key, re-encrypts it with the new key, and prints a `swarm://` rotation manifest reference.

## 3. Create a backup snapshot

```bash
skdb --key /tmp/new.key backup --out /tmp/swarmkeydb-backup.ref
cat /tmp/swarmkeydb-backup.ref
```

Backups return a single immutable `swarm://...` URI. When encryption is enabled, the backup payload is protected with the same Ethereum-derived key so the snapshot can be stored off-band.

## 4. Restore into a fresh instance

Point `skdb` at a new data directory/home and restore with the matching key:

```bash
skdb restore --ref "$(cat /tmp/swarmkeydb-backup.ref)" --key /tmp/new.key
skdb --key /tmp/new.key get your:key
```

## 5. SDK wrappers

The Redis SDKs expose matching management helpers:

- JavaScript: `backup()`, `restore(ref, key)`, `rotateKey(oldKey, newKey)`
- Python: `backup()`, `restore(ref, key)`, `rotate_key(old_key, new_key)`
- Go: `Backup(ctx)`, `Restore(ctx, ref, key)`, `RotateKey(ctx, oldKey, newKey)`

These helpers forward to the SwarmKeyDb management commands exposed by the server.
