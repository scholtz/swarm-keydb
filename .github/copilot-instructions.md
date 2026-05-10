# Copilot instructions

- Keep this repository focused on a C# Redis-protocol key-value server backed by Swarm storage.
- Prefer small, test-covered changes and run `dotnet build SwarmKeyDb.slnx` plus `dotnet test tests/SwarmKeyDb.Tests/SwarmKeyDb.Tests.csproj` before submitting code changes.
- The test project uses **NUnit 4.x** as the required test framework. Always use `[TestFixture]`, `[Test]`, `[OneTimeSetUp]`, `[OneTimeTearDown]`, `[Category]`, and `[TestCase]` attributes — never xUnit `[Fact]`/`[Theory]`. Example:
  ```csharp
  [TestFixture]
  [Category("Integration")]
  public class MyCommandTests
  {
      [Test]
      public async Task MyCommand_ValidInput_ReturnsExpected()
      {
          // arrange, act, assert
      }

      [Test]
      public async Task MyCommand_MissingKey_ReturnsNil()
      {
          // edge-case / negative test
      }
  }
  ```
- Each new subsystem or Redis command family gets its own `*Tests.cs` file. Existing topic files: `StringCommandTests`, `ExpiryCommandTests`, `StreamCommandTests`, `PubSubCommandTests`, `TransactionCommandTests`, `ScriptingCommandTests`, `ScriptReplicationTests`, `ConsistencyTests`, `OperabilityTests`, `EncryptionAclTests`, `CliTests`, `OfflineCrdtTests`, `BatchShardTests`, `CoreStoreTests`. Shared helpers live in `TestHelpers.cs` and stubs in `TestStubs.cs`; import with `using static SwarmKeyDb.Tests.TestHelpers;`.
- Run `dotnet test --filter Category=Unit` for fast local feedback and `dotnet test --filter Category=Integration` for the full integration suite.
- Update the documentation in `docs/` and any impacted top-level docs whenever behavior, configuration, deployment, or developer workflow changes.
- Treat delivery as incomplete if implementation, tests, roadmap status, and product-definition status are not all updated together when scope changes.
- For roadmap-linked work, explicitly verify and update `ROADMAP.md` and `PRODUCT-DESCRIPTION.md` in the same change so delivered and unresolved items stay accurate.
- Include at least one negative/edge-case regression test for every new Redis-visible behavior (arity/syntax errors, invalid numeric ranges, missing keys/groups, and protocol response shape).
- Before finalizing, add a short root-cause note in the PR/summary when quality gaps are found (for example: missing docs alignment, missing edge-case tests, or missing telemetry assertions) and list the preventive change.
- Do not commit local data directories, build output, or postage stamp identifiers.
- Use the `BeeSwarmClient` for production Swarm uploads and the file/in-memory clients only for local development and tests.
- For Redis command work, add tests for both success and error/edge cases (arity, invalid TTL/ranges, missing keys, and RESP encoding details).
- Keep Redis error responses stable and explicit; avoid surfacing raw framework exception text for protocol-visible failures.
