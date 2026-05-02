// nats-client.js — vanilla ES module that wraps nats.ws for the
// Hugin web app.
//
// Two responsibilities:
//
//  1. Talk to hugin-api over HTTPS to obtain NATS credentials —
//     either by registering as an owner (POST /v1/agents/register)
//     or by redeeming a sharing token (POST /v1/share/connect).
//
//  2. Open a WSS connection to the NATS broker with those creds,
//     and expose request/reply + subscribe primitives the workbench
//     UI can consume.
//
// This file is import-only — it doesn't wire itself into any page.
// The integration lives in a follow-up task.
//
// Subject conventions (kept in lock-step with the agent):
//   agent.<id>.req.<method>     — workbench → agent
//   agent.<id>.resp.<reqID>     — agent  → workbench (reply-inbox)
//   agent.<id>.event.<kind>     — agent  → workbench (live events)
//
// kind ∈ {emission, scan-progress, run-lua-progress, presence}
// method ∈ {scan, probe, run-lua}

// nats.ws is shipped as ESM on jsDelivr. Pinning to v1 — the API has
// been stable since 1.x.
import { connect as natsConnect, StringCodec, JSONCodec } from "https://cdn.jsdelivr.net/npm/nats.ws@1/+esm";

const sc = StringCodec();
const jc = JSONCodec();

/**
 * Default request timeout for request/reply, in ms. Long enough for
 * a Lua run with a slow Modbus device, short enough that a dead
 * agent surfaces as an error instead of a hung UI.
 */
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/**
 * fetchAgentCreds — log in as an agent owner.
 *
 * Calls POST {apiBase}/v1/agents/register with the user's GitHub
 * Bearer. Returns the credential bundle the connect() function
 * accepts directly, plus the assigned agent_id.
 *
 * @param {string} apiBase     e.g. "https://api.hugin.sourceful-labs.net"
 * @param {string} ghToken     GitHub OAuth access token
 * @returns {Promise<{natsUrl: string, creds: object, agentId: string, accountJwt?: string}>}
 */
export async function fetchAgentCreds(apiBase, ghToken) {
  const resp = await fetch(`${stripSlash(apiBase)}/v1/agents/register`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${ghToken}`,
      "Content-Type": "application/json",
    },
    body: "{}",
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`register failed: ${resp.status} ${body}`);
  }
  const data = await resp.json();
  return {
    natsUrl: data.nats_url,
    accountJwt: data.account_jwt,
    agentId: data.agent_id,
    creds: {
      jwt: data.user_jwt,
      seed: data.user_seed,
    },
  };
}

/**
 * fetchSharedAgent — redeem a share token.
 *
 * Calls POST {apiBase}/v1/share/connect with the share-token JSON.
 * No GitHub auth needed — the token itself is the auth.
 *
 * @param {string} apiBase
 * @param {string} shareToken
 * @returns {Promise<{natsUrl: string, creds: object, agentId: string, scope: string, accountJwt?: string}>}
 */
export async function fetchSharedAgent(apiBase, shareToken) {
  const resp = await fetch(`${stripSlash(apiBase)}/v1/share/connect`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ share_token: shareToken }),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`share connect failed: ${resp.status} ${body}`);
  }
  const data = await resp.json();
  return {
    natsUrl: data.nats_url,
    accountJwt: data.account_jwt,
    agentId: data.agent_id,
    scope: data.scope,
    creds: {
      jwt: data.user_jwt,
      seed: data.user_seed,
    },
  };
}

/**
 * connect — open a NATS WSS connection.
 *
 * @param {object}   args
 * @param {string}   args.natsUrl  e.g. "wss://nats.hugin.sourceful-labs.net"
 * @param {object}   args.creds    {jwt, seed} from fetchAgentCreds / fetchSharedAgent
 * @returns {Promise<HuginNatsConnection>}
 */
export async function connect({ natsUrl, creds }) {
  if (!natsUrl) throw new Error("natsUrl required");
  if (!creds || !creds.jwt || !creds.seed) {
    throw new Error("creds.jwt and creds.seed required");
  }

  // Translate our credential shape into nats.ws's authenticator
  // contract. nats.ws exposes a jwtAuthenticator helper but it
  // needs the seed as a Uint8Array; we wrap manually so we don't
  // pull in TextEncoder polyfills the user might not have.
  const seedBytes = new TextEncoder().encode(creds.seed);
  const authenticator = (nonce) => {
    // Lazy-import nkeys so we only pay the cost when we actually
    // dial. nats.ws ships nkeys.js alongside.
    return loadNkeys().then((nkeys) => {
      const kp = nkeys.fromSeed(seedBytes);
      const sig = kp.sign(nonce);
      return { jwt: creds.jwt, sig };
    });
  };

  const nc = await natsConnect({
    servers: natsUrl,
    authenticator,
    name: "hugin-web",
    timeout: 10_000,
    reconnect: true,
    reconnectTimeWait: 2_000,
    maxReconnectAttempts: -1,
    waitOnFirstConnect: true,
  });

  return new HuginNatsConnection(nc);
}

/**
 * HuginNatsConnection wraps a nats.ws NatsConnection with the
 * narrower API surface the workbench needs.
 */
class HuginNatsConnection {
  constructor(nc) {
    this._nc = nc;
    this._stateListeners = new Set();
    this._currentState = "connected";
    this._watchStatus(); // fire-and-forget loop
  }

  /**
   * request — request/reply against an agent subject.
   *
   * @param {string} subject  full NATS subject e.g. "agent.agt_42.req.scan"
   * @param {object} body     JSON-serialisable
   * @param {object} [opts]
   * @param {number} [opts.timeout]  ms
   * @returns {Promise<{response: object}>}
   */
  async request(subject, body, opts = {}) {
    const timeout = opts.timeout || DEFAULT_REQUEST_TIMEOUT_MS;
    const msg = await this._nc.request(subject, jc.encode(body || {}), { timeout });
    let response;
    try {
      response = jc.decode(msg.data);
    } catch (err) {
      // Agent wrote raw bytes? Surface what we can.
      response = { _raw: sc.decode(msg.data), _decode_error: String(err) };
    }
    return { response };
  }

  /**
   * subscribe — listen for events.
   *
   * Subjects support NATS wildcards: "agent.agt_42.event.>" subscribes
   * to every event kind for that agent.
   *
   * @param {string}   subject
   * @param {function} callback  invoked as callback(payload, msg)
   * @returns {function}         call to unsubscribe
   */
  subscribe(subject, callback) {
    const sub = this._nc.subscribe(subject);
    (async () => {
      for await (const m of sub) {
        let payload;
        try {
          payload = jc.decode(m.data);
        } catch {
          payload = { _raw: sc.decode(m.data) };
        }
        try {
          callback(payload, m);
        } catch (err) {
          // We never want a UI bug to crash the iteration loop.
          console.error("nats-client subscribe callback threw:", err);
        }
      }
    })().catch((err) => {
      // Iterator-stop is normal on close; only surface real errors.
      if (!this._nc.isClosed()) {
        console.error("nats-client subscribe loop error:", err);
      }
    });
    return () => sub.unsubscribe();
  }

  /**
   * onState — register a connection-state listener. Callbacks are
   * invoked with one of:
   *   "connecting" | "connected" | "disconnected" | "error" | "closed"
   *
   * Returns an unsubscribe function.
   */
  onState(cb) {
    this._stateListeners.add(cb);
    // Fire current state immediately so callers don't race.
    queueMicrotask(() => cb(this._currentState));
    return () => this._stateListeners.delete(cb);
  }

  /**
   * close — drain the connection. Idempotent.
   */
  async close() {
    try {
      await this._nc.drain();
    } catch (_err) {
      // Drain can throw if already closed; ignore.
    }
    this._setState("closed");
  }

  // --- internals ---

  _setState(s) {
    this._currentState = s;
    for (const cb of this._stateListeners) {
      try { cb(s); } catch (err) { console.error("state listener threw:", err); }
    }
  }

  async _watchStatus() {
    // nats.ws exposes a status() async iterator that yields one
    // event per status change. We translate into our simpler
    // string vocabulary.
    try {
      for await (const s of this._nc.status()) {
        switch (s.type) {
          case "reconnecting":
          case "ldm":         // lame-duck-mode, treat as transient
            this._setState("connecting");
            break;
          case "reconnect":
            this._setState("connected");
            break;
          case "disconnect":
            this._setState("disconnected");
            break;
          case "error":
            this._setState("error");
            break;
          case "close":
            this._setState("closed");
            return;
        }
      }
    } catch (err) {
      // Status iterator finishes on close; not really an error.
      if (!this._nc.isClosed()) {
        console.warn("nats-client status iterator ended early:", err);
      }
    }
  }
}

// ---------- helpers ----------

/**
 * subjectFor — build a fully-qualified subject for a given agent.
 *
 *   subjectFor("agt_42", "req.scan")        // "agent.agt_42.req.scan"
 *   subjectFor("agt_42", "event.presence")  // "agent.agt_42.event.presence"
 *   subjectFor("agt_42", "event.>")         // wildcard
 */
export function subjectFor(agentId, suffix) {
  return `agent.${agentId}.${suffix}`;
}

function stripSlash(s) {
  return s.endsWith("/") ? s.slice(0, -1) : s;
}

// nkeys is bundled with nats.ws but exposed under a sub-path. We
// load it lazily and cache; first dial pays a small extra HTTP cost.
let _nkeysPromise = null;
function loadNkeys() {
  if (_nkeysPromise) return _nkeysPromise;
  _nkeysPromise = import("https://cdn.jsdelivr.net/npm/nkeys.js@1/+esm");
  return _nkeysPromise;
}
