import { useEffect, useMemo, useState } from 'react';
import { createClient } from '../../../../sdk/js/dist/index.js';

export default function App() {
  const [items, setItems] = useState<string[]>([]);
  const client = useMemo(() => createClient({ wsUrl: 'ws://127.0.0.1:8765/' }), []);

  useEffect(() => {
    client.connect().then(async () => {
      const raw = await client.get('todos');
      setItems(raw ? JSON.parse(raw) : []);
    });
    return () => { client.disconnect(); };
  }, [client]);

  async function addTodo() {
    const next = [...items, `todo-${Date.now()}`];
    setItems(next);
    await client.set('todos', JSON.stringify(next));
  }

  return (
    <main>
      <h1>SwarmKeyDb React Todo</h1>
      <button onClick={addTodo}>Add todo</button>
      <ul>{items.map((item) => <li key={item}>{item}</li>)}</ul>
    </main>
  );
}
