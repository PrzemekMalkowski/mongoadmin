# 🍃 MongoAdmin

> A lightweight, single-binary web admin panel for MongoDB — replica sets and sharded clusters included.

[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Go](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go)](https://go.dev/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4.2%2B-00ED64?logo=mongodb)](https://www.mongodb.com/)

MongoAdmin is a zero-dependency, self-hosted MongoDB administration interface written in Go.  
Connect to any standalone instance, replica set, or sharded cluster with a single URI and get an instant overview of your cluster health, operations, data distribution, users, and more — all through a clean dark-mode UI.

> [!WARNING]
> **MongoAdmin is currently in beta.** It is not recommended for production use.
> Expect breaking changes between versions. Always test in a safe environment before
> connecting to any live data. Use at your own risk.
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
| **Current Ops** | Live operation list with runtime, plan summary, lock wait, app name — expand any row for full command and metadata |
| **Kill Operations** | Single-op kill or batch kill with filters (op type, namespace, app name, minimum runtime) |
| **Slow Query Profiler** | Get/set profiling level per database, browse `system.profile` with filters |
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

## 📸 Screenshots

<table>
  <tr>
    <td><img src="docs/screenshots/rs_status.png" alt="Replica Set Status" width="280"/></td>
    <td><img src="docs/screenshots/server_status.png" alt="Server Status" width="280"/></td>
    <td><img src="docs/screenshots/ops.png" alt="Operations" width="280"/></td>
  </tr>
  <tr>
    <td align="center"><em>Replica Set Status</em></td>
    <td align="center"><em>Server Status</em></td>
    <td align="center"><em>Operations</em></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/db_stats.png" alt="Database Stats" width="280"/></td>
    <td><img src="docs/screenshots/col_stats.png" alt="Collection Stats" width="280"/></td>
  </tr>
  <tr>
    <td align="center"><em>Database Stats</em></td>
    <td align="center"><em>Collection Stats</em></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/slow_queries.png" alt="Slow Queries" width="280"/></td>
    <td><img src="docs/screenshots/log_view.png" alt="Log View" width="280"/></td>
  </tr>
  <tr>
    <td align="center"><em>Slow Queries</em></td>
    <td align="center"><em>Log View</em></td>
  </tr>
</table>
---

## 🚀 Getting Started

### Prerequisites

- **Go 1.21+** (for building from source)
- A running **MongoDB 4.2+** instance (standalone, replica set, or sharded cluster)

### Build

```bash
git clone https://github.com/your-username/mongoadmin.git
cd mongoadmin
go build -o mongoadmin .
```

### Run

```bash
# Plain HTTP on port 8080 (default)
./mongoadmin

# Custom port
./mongoadmin --tcp_port 9090

# HTTPS with an auto-generated self-signed certificate
./mongoadmin --tls

# HTTPS with your own certificate
./mongoadmin --tls --cert /path/to/cert.pem --key /path/to/key.pem

# Enable verbose server-side debug logging
./mongoadmin --debug
```

Open your browser at `http://localhost:8080` (or `https://localhost:8080` if TLS is enabled).

### Command-line flags

| Flag | Default | Description |
|------|---------|-------------|
| `--tcp_port` | `8080` | TCP port to listen on |
| `--tls` | `false` | Enable HTTPS |
| `--cert` | `mongoadmin.crt` | Path to TLS certificate file |
| `--key` | `mongoadmin.key` | Path to TLS private key file |
| `--debug` | `false` | Enable server-side debug logging |
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
| `POST /api/server/status` | `uri` | `db.serverStatus()` |
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
- All destructive operations (kill op, drop index, delete user/role, stop balancer) require explicit user action in the UI.

---

## 🤝 Contributing

Pull requests and issues are welcome!  
Please open an issue first to discuss significant changes.

```bash
# Run locally during development
go run . --debug --tcp_port 8080
```

---

## 📜 License

MongoAdmin is free software: you can redistribute it and/or modify it under the terms of the  
**GNU General Public License version 3** as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but **WITHOUT ANY WARRANTY**;  
without even the implied warranty of **MERCHANTABILITY** or **FITNESS FOR A PARTICULAR PURPOSE**.  
See the [GNU General Public License](https://www.gnu.org/licenses/gpl-3.0) for more details.
