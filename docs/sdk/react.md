# React connector quick reference

Install:

```bash
cd swarm-keydb-react
npm install
```

Run tests:

```bash
npm test
```

Core API:

- `SwarmKeyDbProvider`
- `useSwarmValue(key, options?)`
- `useSwarmPut(key, options?)`
- `useSwarmDelete(key)`
- `useSwarmKeys(prefix?, options?)`

Example app:

```bash
cd examples/react-app
npm install
npm run dev
```
