'use client';

import { useEffect, useMemo, useState } from 'react';
import { createClient } from '../../../../sdk/js/dist/index.js';

export default function Page() {
  const [value, setValue] = useState<string | null>(null);
  const client = useMemo(() => createClient({ wsUrl: 'ws://127.0.0.1:8765/' }), []);

  useEffect(() => {
    client.connect().then(async () => {
      await client.set('nextjs:hello', 'world');
      setValue(await client.get('nextjs:hello'));
    });
    return () => { client.disconnect(); };
  }, [client]);

  return <div>Next.js + SwarmKeyDb value: {value ?? 'loading'}</div>;
}
