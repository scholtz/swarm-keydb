FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY SwarmKeyDb.slnx ./
COPY src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj src/SwarmKeyDb.Migrate/
RUN dotnet restore src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj
COPY src ./src
RUN dotnet publish src/SwarmKeyDb.Migrate/SwarmKeyDb.Migrate.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish ./
ENTRYPOINT ["dotnet", "SwarmKeyDb.Migrate.dll"]
