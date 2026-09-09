# ychat-relay

Relay and archivist server for [ychat](https://github.com/IronDuckDeveloper/ychat-client) — a peer-to-peer encrypted chat application. This service doesn't hold conversations as a central authority; it bootstraps peers, keeps rooms available when clients are offline, and enforces spam/ban policy at the network level.

## Role in ychat

- **Bootstrap peer** — gives `ychat-client` instances a stable libp2p peer to connect through
- **Archivist** — replicates and persists OrbitDB chat rooms so history survives even when no client is online
- **Access control backstop** — mirrors the client's `RateLimitedAccessController`, so rate limits and ownership checks are enforced locally on every peer during `Log.joinEntry`, not by trusting a central server
- **File ownership registry** — tracks uploaded files and supports receiver-side deletion sync
- **Ban management** — sync'd bans across relay instances
- **Session tokens** — issues and verifies stateless client session tokens

## Tech stack

Node.js · Express · Kubo (IPFS) · libp2p · OrbitDB v2 · gossipsub · SQLite · Nginx (security gateway in front of Kubo)

## Architecture notes

- **`RateLimitedAccessController`** (sliding window + character cap) must match the client's implementation — `canAppend` runs identically on both sides so malicious entries are rejected locally instead of relying on central moderation.
- **Ban sync** uses a last-write-wins UPSERT (`ON CONFLICT DO UPDATE ... WHERE excluded.updated_at > banned_users.updated_at`), so an unban can't be clobbered by a stale broadcast arriving late.
- **Session tokens** are HMAC-signed, base64-encoded, and verified with `timingSafeEqual`. `CLIENT_SESSION_SECRET` must be identical across all relay instances for failover to work.
- **OrbitDB manifest immutability** — room type and access controller are fixed at creation time; opening an existing DB by address ignores any options passed afterward.

## Getting started

### Prerequisites

- Node.js
- A running Kubo (IPFS) node
- (Recommended) Nginx configured as a security gateway in front of Kubo

### Environment variables

| Variable | Purpose |
|---|---|
| `CLIENT_SESSION_SECRET` | Shared HMAC secret for signing/verifying session tokens across relay instances |

*(add any additional required env vars here as the project grows)*

### Install

```bash
npm install
```

### Run

```bash
npm start
```

## Project layout (key files)

| File | Purpose |
|---|---|
| `src/index.js` | Server entry point |
| `src/access-controllers/rateLimitedAccessController.js` | Mirrored access controller (rate limiting + ownership) |
| `ArchivistService` | Room replication/persistence |

## License

TBD
