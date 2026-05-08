# Copilot instructions

- Keep this repository focused on a C# Redis-protocol key-value server backed by Swarm storage.
- Prefer small, test-covered changes and run `dotnet build SwarmKeyDb.slnx` plus `dotnet run --project tests/SwarmKeyDb.Tests/SwarmKeyDb.Tests.csproj` before submitting code changes.
- Do not commit local data directories, build output, or postage stamp identifiers.
- Use the `BeeSwarmClient` for production Swarm uploads and the file/in-memory clients only for local development and tests.
- For Redis command work, add tests for both success and error/edge cases (arity, invalid TTL/ranges, missing keys, and RESP encoding details).
- Keep Redis error responses stable and explicit; avoid surfacing raw framework exception text for protocol-visible failures.
