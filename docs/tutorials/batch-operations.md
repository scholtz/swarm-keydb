# Tutorial: Batch Operations

Goal: write/read multiple keys in one workflow.

## C# runnable example

```bash
dotnet run --project examples/BatchOperationsExample/BatchOperationsExample.csproj
```

## Redis protocol batch commands

```bash
redis-cli -p 6379 MSET cfg:a on cfg:b off cfg:c 500
redis-cli -p 6379 MGET cfg:a cfg:b cfg:c
redis-cli -p 6379 MSETNX cfg:a override cfg:d true
```
