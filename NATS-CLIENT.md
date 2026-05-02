# nats-client.js

Vanilla JS module for talking to a Hugin agent over NATS from the browser.

Lives at `/nats-client.js` in this repo, importable with native ESM.

## When to use it

- Browser ↔ remote agent (Pi at home, browser on a laptop anywhere).
- Browser ↔ shared agent (someone shared their agent with you via a share token).
- Anywhere we'd previously `fetch()` against `http://localhost:19090`.

The module does **not** wire itself into any page. Pages opt in by importing it.

## Architecture

```
browser  ──WSS──►  NATS broker  ◄──WSS──  hugin-agent
                       ▲
                       │ (auth-callout, ops-only)
                       │
                  hugin-api signs short-lived NATS user JWTs
```

Subjects:

| Subject                              | Direction          | Purpose                          |
| ------------------------------------ | ------------------ | -------------------------------- |
| `agent.<id>.req.<method>`            | client → agent     | request (request/reply)          |
| `agent.<id>.resp.<request_id>`       | agent → client     | matched reply (auto via inbox)   |
| `agent.<id>.event.emission`          | agent → client     | live driver emissions            |
| `agent.<id>.event.scan-progress`     | agent → client     | live LAN scan updates            |
| `agent.<id>.event.run-lua-progress`  | agent → client     | live Lua test progress           |
| `agent.<id>.event.presence`          | agent → client     | 15s heartbeat                    |

Methods: `scan` · `probe` · `run-lua`.

## Auth model

- **Owner**: GitHub OAuth → `POST /v1/agents/register` → bundle scoped read+write on `agent.<owned>.>`.
- **Share**: hold a share token → `POST /v1/share/connect` → bundle scoped publish-on-req + subscribe-on-event/resp only.

JWTs default to 24h. Reconnects re-use the same JWT until expiry; expiry → re-register.

## Quick start

### As an agent owner

```js
import { fetchAgentCreds, connect, subjectFor }
  from "/nats-client.js";

const ghToken = localStorage.getItem("hugin_gh_token");
const apiBase = "https://api.hugin.sourceful-labs.net";

const { natsUrl, agentId, creds } = await fetchAgentCreds(apiBase, ghToken);
const conn = await connect({ natsUrl, creds });

// Run a scan.
const { response } = await conn.request(
  subjectFor(agentId, "req.scan"),
  { cidr: "192.168.1.0/24", deep_probe: true },
  { timeout: 60_000 },
);
console.log(response.devices);

// Live progress on Lua test.
const unsub = conn.subscribe(
  subjectFor(agentId, "event.run-lua-progress"),
  (evt) => console.log("emission:", evt),
);

await conn.request(
  subjectFor(agentId, "req.run-lua"),
  { lua_source: source, config: { host: "192.168.1.42" }, actions: ["init", "poll"] },
);
unsub();
```

### As a share-token holder

```js
import { fetchSharedAgent, connect, subjectFor }
  from "/nats-client.js";

const shareToken = new URLSearchParams(location.hash.slice(1)).get("share");
const { natsUrl, agentId, creds, scope } =
  await fetchSharedAgent("https://api.hugin.sourceful-labs.net", shareToken);

console.log(`connected to ${agentId} with scope ${scope}`);

const conn = await connect({ natsUrl, creds });
// Same surface as the owner case — but writes will be rejected by the broker
// for share-scoped users.
```

### Connection state

```js
const off = conn.onState((s) => {
  // s ∈ "connecting" | "connected" | "disconnected" | "error" | "closed"
  document.body.dataset.natsState = s;
});
// off() to stop.
```

### Closing

```js
await conn.close();   // idempotent, drains pending publishes
```

## API reference

| Export                    | Type                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------- |
| `connect(args)`           | `({natsUrl, creds}) → Promise<HuginNatsConnection>`                                   |
| `fetchAgentCreds(api,gh)` | `(apiBase, ghToken) → Promise<{natsUrl, agentId, creds}>`                             |
| `fetchSharedAgent(api,t)` | `(apiBase, shareToken) → Promise<{natsUrl, agentId, scope, creds}>`                   |
| `subjectFor(id, suffix)`  | `(agentId, suffix) → string`                                                          |

`HuginNatsConnection`:

| Method                                | Returns                                       |
| ------------------------------------- | --------------------------------------------- |
| `request(subject, body, {timeout})`   | `Promise<{response}>` (response is decoded JSON) |
| `subscribe(subject, cb)`              | `() → void` (call to unsubscribe)             |
| `onState(cb)`                         | `() → void` (call to remove listener)         |
| `close()`                             | `Promise<void>` (idempotent)                  |

## Dependencies

Loaded from jsDelivr at runtime — no npm install, no build step.

- `nats.ws@1` — connection + codecs
- `nkeys.js@1` — signs the auth-callout nonce (lazy-loaded on first dial)

## Status

The module compiles and exports the surface above. It has not been
exercised against a live NATS broker — that's blocked on the broker
deployment task. Once the broker is up:

1. Smoke: open the browser console on a page that imports the module, run the "Quick start" snippet against a registered agent.
2. Validate the subject scheme matches what the agent publishes (it does, by construction — both are derived from `subjectFor`).
3. Wire into `workbench.html` (separate task).
