# 🍃 MongoAdmin

> A lightweight, single-binary web admin panel for MongoDB DBAs — replica sets and sharded clusters auto-discovery included.

[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Go](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go)](https://go.dev/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4.2%2B-00ED64?logo=mongodb)](https://www.mongodb.com/)

MongoAdmin is a zero-dependency, self-hosted MongoDB administration interface written in Go.  
Connect to any standalone instance, replica set, or sharded cluster with a single URI and get an instant overview of your cluster health, operations, data distribution, users, and more — all through a clean dark-mode UI.  
The tool is very lightweight in terms of resource requirements and size (binary around 16MB), so should be ideal as a **sidecar container** to your MongoDB cluster deployment.  
No need for any agents, or SSH access, MongoAdmin connects directly to all MongoDB instances it is able to find through the router (mongos) or replica set member connection string.  
It is meant to fill the gap in DBA-oriented GUI tools, focused on replication, sharding and general quick cluster overview and simple administrative operations.   
MongoAdmin is **not** intended for viewing or editing data collections, there are plenty of other tools available for that. It is also not a long term monitoring or alerting solution. For that, I strongly recommend [Percona Monitoring and Management (PMM)](https://docs.percona.com/percona-monitoring-and-management/3/) for the best monitoring experience.

> [!WARNING]
> **MongoAdmin is currently in beta.** It is not recommended for production use.
> Expect breaking changes between versions. Always test in a safe environment before
> connecting to any live data. Use at your own risk.
---
## 📸 Screenshots

<table>
  <tr>
    <td><a href="docs/screenshots/rs_status.png"><img src="docs/screenshots/rs_status.png" alt="Replica Set Status" width="280"/></a></td>
    <td><a href="docs/screenshots/server_status.png"><img src="docs/screenshots/server_status.png" alt="Server Status" width="280"/></a></td>
    <td><a href="docs/screenshots/ops.png"><img src="docs/screenshots/ops.png" alt="Operations" width="280"/></a></td>
  </tr>
  <tr>
    <td align="center"><em>Replica Set Status</em></td>
    <td align="center"><em>Server Status</em></td>
    <td align="center"><em>Operations</em></td>
  </tr>
  <tr>
    <td><a href="docs/screenshots/db_stats.png"><img src="docs/screenshots/db_stats.png" alt="Database Stats" width="280"/></a></td>
    <td><a href="docs/screenshots/col_stats.png"><img src="docs/screenshots/col_stats.png" alt="Collection Stats" width="280"/></a></td>
    <td><a href="docs/screenshots/sharding_status.png"><img src="docs/screenshots/sharding_status.png" alt="Sharding Status" width="280"/></a></td>
  </tr>
  <tr>
    <td align="center"><em>Database Stats</em></td>
    <td align="center"><em>Collection Stats</em></td>
    <td align="center"><em>Sharding Status</em></td>
  </tr>
  <tr>
    <td><a href="docs/screenshots/slow_queries.png"><img src="docs/screenshots/slow_queries.png" alt="Slow Queries" width="280"/></a></td>
    <td><a href="docs/screenshots/log_view.png"><img src="docs/screenshots/log_view.png" alt="Log View" width="280"/></a></td>
    <td><a href="docs/screenshots/live_traffic.png"><img src="docs/screenshots/live_traffic.png" alt="Live Traffic" width="280"/></a></td>
  </tr>
  <tr>
    <td align="center"><em>Slow Queries</em></td>
    <td align="center"><em>Log View</em></td>
    <td align="center"><em>Live Traffic</em></td>
  </tr>
  <tr>
    <td><a href="docs/screenshots/users_roles.png"><img src="docs/screenshots/users_roles.png" alt="Users & Roles" width="280"/></a></td>
  </tr>
  <tr>
    <td align="center"><em>Users &amp; Roles</em></td>
  </tr>
</table>

---

## ✨ Features

### Replica Set
| Feature | Description |
|---------|-------------|
| **RS Status** | Member states, health, uptime, sync sources, election term |
| **Cluster Diagram** | Visual topology map with per-node version, priority, lag, and one-click navigation |
| **RS Configuration** | Visual config graph, priority bars, vote map, timing settings, write concern |

### Sharded Cluster
| Feature | Description |
|---------|-------------|
| **Topology Discovery** | Auto-detects mongos, shards, and config server from a single URI |
| **Sharding Status** | Shard list, balancer state, write concern, migration results (last 24 h) |
| **Balancer Control** | Start / stop the balancer, view last 5 balancer rounds and their outcome |
| **Chunk Distribution** | Per-collection chunk and data distribution across shards, with a balance score |
| **Database Stats** | Data size, storage, index size, object count, sharded vs. local collections — with per-shard breakdown |

### Operations & Diagnostics
| Feature | Description |
|---------|-------------|
| **Server Status** | Connections, network I/O, document metrics, lock queues, command counters (top 25) |
| **Host Info** | CPU, cores, RAM, OS, open-file limits, NUMA state |
| **Network Compression** | Per-compressor (snappy/zstd/zlib) bytes-in/out with utilisation bars |
| **WiredTiger Stats** | Full WiredTiger, tcmalloc, and memory section from `serverStatus` |
| **Flow Control** | Enable/disable flow control and set target lag per mongod |
| **Live Traffic Monitor** | Real-time, multi-host charts for op counters (inserts/queries/updates/deletes/commands/getmores), connections, network throughput, resident memory, and lock queue depth — configurable polling interval (1–30 s) and rolling time window (5 min – 4 h); uses the lightweight `/api/server/metrics` endpoint so each poll fetches only the requested `serverStatus` sections (~2–4 KB instead of ~300–500 KB); data stays in browser memory only |
| **Current Ops** | Live operation list with runtime, plan summary, lock wait, app name — expand any row for full command and metadata |
| **Kill Operations** | Single-op kill or batch kill with filters (op type, namespace, app name, minimum runtime) |
| **Slow Query Profiler** | Get/set profiling level per database, browse `system.profile` with filters, and run **EXPLAIN** on any captured query in a pop-up — strips session/routing metadata automatically, supports `queryPlanner`, `executionStats`, and `allPlansExecution` verbosity, with a summary card flagging COLLSCAN, IXSCAN, and in-memory sorts |
| **Log Viewer** | View `global`, `rs`, or `startupWarnings` log lines |

### Data & Indexes
| Feature | Description |
|---------|-------------|
| **Collection Stats** | Count, data size, storage size, index size, shard key — across all collections in a DB |
| **Index Management** | List indexes with usage counters, sizes, TTL, hidden/partial/sparse flags; drop indexes (shard-key index protection) |
| **Oplog Stats** | Oplog size, usage %, retention window (hours), and per-member replication lag |

### Security
| Feature | Description |
|---------|-------------|
| **Users & Roles** | View, create, and delete users; assign/revoke roles; create and update custom roles with privilege builder |
| **Per-Shard Auth** | Prompt for shard-specific credentials when a shard has different auth from the mongos |
| **Password Masking** | Connection URIs are masked in the browser UI — passwords are never displayed |

---

## 🚀 Getting Started

### Prerequisites

- **Go 1.21+** (for building from source)
- A running **MongoDB 4.2+** instance (standalone, replica set, or sharded cluster)

### Build

```bash
git clone https://github.com/PrzemekMalkowski/mongoadmin.git
cd mongoadmin
go build -o mongoadmin .
```

### Run

```bash
# Plain HTTP on port 8787 (default)
./mongoadmin

# Custom port
./mongoadmin --tcp_port 9090

# HTTPS with an auto-generated self-signed certificate
./mongoadmin --tls

# HTTPS with your own certificate
./mongoadmin --tls --cert /path/to/cert.pem --key /path/to/key.pem

# Enable verbose server-side debug logging
./mongoadmin --debug

# Read-only mode — all write/mutating operations are disabled in the UI and blocked at the API level
./mongoadmin --view-only
```

Open your browser at `http://localhost:8787` (or `https://localhost:8787` if TLS is enabled).

### Command-line flags

| Flag | Default | Description |
|------|---------|-------------|
| `--tcp_port` | `8787` | TCP port to listen on |
| `--tls` | `false` | Enable HTTPS |
| `--cert` | `mongoadmin.crt` | Path to TLS certificate file |
| `--key` | `mongoadmin.key` | Path to TLS private key file |
| `--debug` | `false` | Enable server-side debug logging |
| `--view-only` | `false` | Disable all write/mutating operations — destructive UI controls are hidden and the API returns `403` for any write attempt |
| `--version` | — | Print version and exit |

---

## 🔌 Connecting

Paste a standard MongoDB connection URI into the input field and click **Connect**:

```
mongodb://username:password@host:27017
mongodb://username:password@host:27017/?replicaSet=myRS
mongodb://username:password@mongos1:27017,mongos2:27017/
mongodb+srv://username:password@cluster.example.mongodb.net/
```

- Credentials are masked in the UI after connecting.
- For **sharded clusters**, connect to a **mongos** router — MongoAdmin will automatically discover all shards and the config server.
- For **replica sets**, connect to any member; MongoAdmin will build the full topology.
- If individual shards require different credentials from the mongos, MongoAdmin will prompt you per shard.

---

## 🔑 Required MongoDB Privileges

MongoAdmin authenticates as a regular MongoDB user — there is no separate user database inside the tool. Every dashboard runs admin commands on your behalf, so the connected user must hold the privileges those commands require. The tables below break the privilege requirements down per dashboard so you can grant the minimum set that fits your use case.

> [!NOTE]
> In a **sharded cluster**, create the user on a `mongos` against the `admin` database — MongoDB writes it to the config servers and it is then valid cluster-wide. Create users only on each shard's primary when you need shard-local accounts (the per-shard authentication prompt in MongoAdmin uses these).

### Per-dashboard privilege map

| Dashboard / Feature | Built-in role(s) needed | Underlying commands / actions |
|---------------------|-------------------------|------------------------------|
| Topology Discovery, RS Status, Cluster Diagram | `clusterMonitor` | `listShards`, `getCmdLineOpts`, `replSetGetStatus`, `replSetGetConfig`, find on `config.mongos`, `$currentOp` |
| RS Configuration — **view** | `clusterMonitor` | `replSetGetConfig`, `getDefaultRWConcern` |
| RS Configuration — **apply changes** | `clusterManager` | `replSetReconfig`, `setDefaultRWConcern` |
| Sharding Status, Chunk Distribution, Database Stats | `clusterMonitor` | `balancerStatus`, `listDatabases`, find / aggregate on `config.collections`, `config.chunks`, `config.databases`, `config.changelog` |
| Balancer Control | `clusterManager` | `balancerStart`, `balancerStop` |
| Collection Stats, Index Management — **list** | `readAnyDatabase` *(or `dbAdminAnyDatabase`)* | `dbStats`, `collStats`, `listCollections`, `listIndexes`, `$indexStats` |
| Index Management — **drop index** | `dbAdminAnyDatabase` | `dropIndex` |
| Server Status, Host Info, Network Compression, WiredTiger, Top Commands, Live Traffic Monitor | `clusterMonitor` | `serverStatus`, `hostInfo`, `top`, `getParameter` |
| Flow Control — **set** | `hostManager` | `setParameter` (`enableFlowControl`, `flowControlTargetLagSeconds`) |
| Current Ops | `clusterMonitor` | `$currentOp` with `inprog` action |
| Kill Operations | `hostManager` | `killop` |
| Slow Queries — **browse `system.profile`** | `clusterMonitor` | find on `<db>.system.profile` |
| Slow Queries — **change profiling level** | `dbAdminAnyDatabase` | `profile` command (`enableProfiler`) |
| Slow Queries — **EXPLAIN on user collection** | `readAnyDatabase` | privileges of the underlying op (`find`, `aggregate`, `count`, …); explaining `update` / `delete` additionally requires write privileges on the target collection |
| Log Viewer | `clusterMonitor` | `getLog` |
| Oplog Stats | `clusterMonitor` + `read` on `local` | `replSetGetStatus`, `serverStatus`, `collStats` and find on `local.oplog.rs` |
| Users & Roles | `userAdminAnyDatabase` | `usersInfo`, `rolesInfo`, `createUser`, `dropUser`, `updateUser`, `createRole`, `updateRole`, `dropRole` |

### Recommended `madmin` user — full functionality

The set below covers **every** MongoAdmin dashboard except explain of `update` / `delete` ops on user collections (which would also require write privileges — see the note below). It deliberately avoids `root` and `clusterAdmin` so the user cannot drop databases.

Run this on a `mongos` (for sharded) or any RS member's primary (for replica sets):

```js
use admin
db.createUser({
  user: "madmin",
  pwd:  passwordPrompt(),   // or a literal string
  roles: [
    { role: "clusterMonitor",       db: "admin" },  // monitoring, RS / sharding status, getLog, find on system.profile and config.*
    { role: "clusterManager",       db: "admin" },  // balancer, default RWC, replSetReconfig
    { role: "hostManager",          db: "admin" },  // killOp, setParameter (flow control)
    { role: "dbAdminAnyDatabase",   db: "admin" },  // drop index, profile command, dbStats / collStats on any DB
    { role: "readAnyDatabase",      db: "admin" },  // explain on user collections, find on any DB
    { role: "userAdminAnyDatabase", db: "admin" },  // user / role management
    { role: "read",                 db: "local" }   // oplog.rs (Oplog Stats dashboard)
  ]
})
```

Then connect with:

```
mongodb://madmin:<password>@<host>:27017/?authSource=admin
```

> [!TIP]
> If you also want to **EXPLAIN** `update` and `delete` operations from the profiler (a niche case — explain on writes still requires the underlying write privileges even though no rows are touched), add `{ role: "readWriteAnyDatabase", db: "admin" }` to the role list. Otherwise the Explain button on `update` / `remove` profile rows will return an authorization error, while everything else keeps working.

### Recommended `madmin-ro` user — read-only / monitoring only

For operators who should only **view** the cluster, pair this user with the `--view-only` flag so destructive UI controls are also hidden:

```js
use admin
db.createUser({
  user: "madmin-ro",
  pwd:  passwordPrompt(),
  roles: [
    { role: "clusterMonitor",  db: "admin" },
    { role: "readAnyDatabase", db: "admin" },
    { role: "read",            db: "local" }
  ]
})
```

This account can use every monitoring-only dashboard (RS Status, Cluster Diagram, Sharding Status, Server Status, Host Info, Live Traffic, Current Ops, Slow Queries with EXPLAIN, Log Viewer, Oplog Stats, Database / Collection Stats, Index list). All write-back features (balancer toggle, RS reconfig, flow control, kill op, drop index, set profile level, user / role management) will be refused both by MongoAdmin (when launched with `--view-only`) and by MongoDB itself (insufficient privileges).

### Quick alternative — `root`

If you don't mind giving the tool full superuser rights (typical for a DBA's own session), `root` covers everything in one role:

```js
db.createUser({ user: "madmin", pwd: passwordPrompt(), roles: [ { role: "root", db: "admin" } ] })
```

This is the simplest option but the least defensible from a least-privilege standpoint.

---

## 🗂 Project Structure

```
mongoadmin/
├── main.go              # Go backend — HTTP server and all API handlers
└── templates/
    └── index.html       # Single-page frontend (Tailwind CSS, vanilla JS)
```

The entire application ships as a single self-contained binary plus the `templates/` directory.  
No database, no configuration file, and no external services are required to run.

---

## 🛠 API Reference

All endpoints accept `POST` requests with `application/x-www-form-urlencoded` bodies and return `application/json`.

### Core

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/ping` | `uri` | Test connectivity, returns latency in ms |
| `POST /api/topology` | `uri` | Discover cluster topology (sharded / replica set) |

### Replica Set

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/rs/status/all` | `uri`, `rs_uri[]` | `replSetGetStatus` — fan-out across all RS URIs |
| `POST /api/rs/config` | `uri` | `replSetGetConfig` |
| `POST /api/rs/members` | `uri`, `rs_uri[]` | List individual mongod members with direct-connection URIs |
| `POST /api/rs/member/patch` | `uri`, `member_id`, `priority`, `hidden`, `votes`, `secondaryDelaySecs` | Reconfigure a single RS member |
| `POST /api/rs/flow-control` | `uri`, `enable`, `targetLagSecs` | Enable/disable flow control or adjust target lag |

### Sharding

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/sh/status` | `uri` | Sharding status — balancer, shards, databases, migration history |
| `POST /api/sh/balancer` | `uri`, `enable` | Start (`true`) or stop (`false`) the balancer |
| `POST /api/sh/write-concern` | `uri`, `w`, `j`, `wtimeout` | Set default write concern |

### Server

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/server/status` | `uri` | `db.serverStatus()` — full output |
| `POST /api/server/metrics` | `uri`, `sections` | Lightweight partial `serverStatus` — pass a comma-separated list of top-level sections (e.g. `opcounters,connections,network`) and the server suppresses all other expensive sections; used by the Live Traffic Monitor poller |
| `POST /api/server/hostinfo` | `uri` | `hostInfo` + memory from `serverStatus` |
| `POST /api/server/top-commands` | `uri` | Top 25 commands by call count |
| `POST /api/server/getparam` | `uri`, `param` | `getParameter` for a named parameter |

### Databases & Collections

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/db/stats` | `uri` | Database list with sizes, collection counts, per-shard distribution |
| `POST /api/db/coll-stats` | `uri`, `db` | Collection stats for every collection in a database |
| `POST /api/db/sharded-collections` | `uri`, `db` | List sharded collections in a database |
| `POST /api/db/chunk-distribution` | `uri`, `ns` | Chunk and data distribution for a sharded namespace |
| `POST /api/db/indexes` | `uri`, `db`, `collection` | List indexes with usage stats and sizes |
| `POST /api/db/drop-index` | `uri`, `db`, `collection`, `name` | Drop a named index |
| `POST /api/db/oplog-stats` | `uri`, `rs_uri[]` | Oplog size, usage, retention window, and member lag |

### Operations

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/current-op` | `uri` | `$currentOp` — all users, idle connections, idle cursors |
| `POST /api/kill-op` | `uri`, `opid` | Kill a single operation |
| `POST /api/kill-op` | `uri`, `batch=true`, `op`, `ns`, `app`, `min_secs` | Batch kill with filters |

### Profiler & Logs

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/db/profiler-level` | `uri`, `db` | Get current profiling level |
| `POST /api/db/profiler-level-set` | `uri`, `db`, `level`, `slowMs` | Set profiling level and threshold |
| `POST /api/db/profiler-entries` | `uri`, `db`, `ns`, `op`, `minMs`, `limit` | Query `system.profile` |
| `POST /api/db/explain` | `uri`, `db`, `command` (JSON), `op`, `ns`, `verbosity` | Run `explain` on a profiled command — strips driver metadata (`lsid`, `$clusterTime`, `$db`, etc.), reorders the inner command so the explainable op (`find` / `aggregate` / `count` / `distinct` / `findAndModify` / `update` / `delete` / `mapReduce`) is the first key, and returns the raw `explain` output. `verbosity` accepts `queryPlanner`, `executionStats` (default), or `allPlansExecution` |
| `POST /api/db/log` | `uri`, `kind` | `getLog` (global / rs / startupWarnings) |
| `POST /api/db/wiredtiger` | `uri` | WiredTiger, tcmalloc, and memory sections |

### Users & Roles

| Endpoint | Key Parameters | Description |
|----------|---------------|-------------|
| `POST /api/user/users` | `uri`, `db` | List users in a database |
| `POST /api/user/create-user` | `uri`, `db`, `username`, `password`, `roles` | Create a user |
| `POST /api/user/delete-user` | `uri`, `db`, `username` | Drop a user |
| `POST /api/user/set-user-roles` | `uri`, `db`, `username`, `roles` | Replace a user's roles |
| `POST /api/user/update-user-roles` | `uri`, `db`, `username`, `roles` | Grant additional roles |
| `POST /api/user/roles` | `uri`, `db`, `showBuiltinRoles` | List roles in a database |
| `POST /api/user/role-detail` | `uri`, `db`, `roleName` | Full privileges for one role |
| `POST /api/user/create-role` | `uri`, `db`, `roleName`, `privileges`, `inheritedRoles` | Create a custom role |
| `POST /api/user/update-role` | `uri`, `db`, `roleName`, `privileges`, `inheritedRoles` | Update a custom role |
| `POST /api/user/delete-role` | `uri`, `db`, `roleName` | Drop a custom role |

---

## 🔒 Security Considerations

MongoAdmin is designed as an **internal / ops tool**. Before exposing it:

- **Do not expose MongoAdmin on a public interface** without authentication in front of it (e.g. reverse proxy with HTTP Basic Auth, VPN, or SSH tunnel).
- Enable TLS (`--tls`) whenever the tool is accessed over an untrusted network.
- MongoAdmin does not implement its own login system — it relies on MongoDB's built-in access control via the connection URI.
- Use `--view-only` when granting access to operators or teams who need visibility into the cluster but must not be able to alter it. In this mode all destructive UI controls are hidden and every mutating API endpoint returns `403 Forbidden` regardless of the MongoDB user's own privileges.
- All destructive operations (kill op, drop index, delete user/role, stop balancer) require explicit user action in the UI.

---

## 🤝 Contributing

Pull requests and issues are welcome!  
Please open an issue first to discuss significant changes.

```bash
# Run locally during development
go run . --debug --tcp_port 8787
```

---

## 📜 License

MongoAdmin is free software: you can redistribute it and/or modify it under the terms of the  
**GNU General Public License version 3** as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but **WITHOUT ANY WARRANTY**;  
without even the implied warranty of **MERCHANTABILITY** or **FITNESS FOR A PARTICULAR PURPOSE**.  
See the [GNU General Public License](https://www.gnu.org/licenses/gpl-3.0) for more details.
