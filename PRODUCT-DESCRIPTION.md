# A Key-Value Store on Swarm using redis protocol in C# and build to docker

## Description

Build a developer-friendly Key-Value database library on top of Swarm — a familiar `get(key)` / `put(key, value)` interface backed by decentralized, persistent storage.

Swarm has powerful primitives — content addressing, feeds, manifests — but using them today requires knowing how all the pieces fit together. Most developers don't want to think about feeds and topics and single-owner chunks. They want a simple database. This library wraps Swarm's internals into something any developer can pick up in minutes.

Under the hood, Swarm Feeds let you create a stable pointer (identified by your address + a topic string) that you can update over time. Map each "key" to a topic, and you've got a KV store. For listing keys, Swarm manifests can serve as an index.

***What is required to complete this bounty?***

- Support strings, JSON, and binary values
- Key listing and iteration
- Handle storage costs (postage stamps) transparently
- Clear docs with working examples

***What are examples of use cases you are looking to solve?***

- User profiles and settings for decentralized apps
- Config storage for dApps that need mutable state
- Chat history, bookmarks, preferences — anything an app would normally put in a database

***What are the UX, Privacy, other requirements?***

- The developer should never need to understand feeds, topics, or SOCs to use this library
- Data is tied to the user's Ethereum keypair — private by default

## Judging Criteria

- **Developer experience:** How easy is it to get started? Could someone use this in 5 minutes?
- **API design:** Clean, intuitive, well-documented
- **Completeness:** Supports listing, deletion, and iteration — not just get/put
- **Edge cases:** Handles missing keys and large values gracefully. *Nice to have:* a clever approach to concurrent writes (not trivial with the current protocol — impress us if you solve it)
- **Examples:** Working, runnable examples included

## Resources

**Contacts:** Swarm team at the booth + [Discord](https://discord.gg/dUS68y87U4)

**Resource Links:**

- [Swarm Feeds guide](https://docs.ethswarm.org/docs/develop/access-the-swarm/feeds)
- [Dynamic content guide](https://docs.ethswarm.org/docs/develop/dynamic-content) — practical feed examples
- [bee-js SDK](https://github.com/ethersphere/bee-js) — `FeedWriter`, `FeedReader`, `MantarayNode`
- [Swarm docs](https://docs.ethswarm.org/docs/develop/introduction/)
