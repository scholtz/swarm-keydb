FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY SwarmKeyDb.slnx ./
COPY src/SwarmKeyDb/SwarmKeyDb.csproj src/SwarmKeyDb/
COPY src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj src/SwarmKeyDb.Server/
RUN dotnet restore src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj
COPY src ./src
RUN dotnet publish src/SwarmKeyDb.Server/SwarmKeyDb.Server.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENV SWARM_KEYDB_BIND=0.0.0.0 \
    SWARM_KEYDB_PORT=6379 \
    SWARM_KEYDB_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 6379
ENTRYPOINT ["dotnet", "SwarmKeyDb.Server.dll"]
