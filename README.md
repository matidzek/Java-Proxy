# Java TCP/UDP Proxy Server

A multithreaded network proxy server written in Java that supports simultaneous TCP and UDP communication, dynamic key-to-server mapping, and transparent forwarding in tree topologies.

---

## Overview

This proxy acts as an intermediary between clients and one or more backend servers. On startup it discovers all configured nodes (detecting whether each is a plain server or another proxy), collects their stored keys, and builds a routing map. Incoming GET/SET requests are then forwarded to the correct backend transparently — regardless of whether the client or the backend uses TCP or UDP.

---

## Features

- Simultaneous **TCP and UDP** listener on a single port
- Automatic **TCP → UDP fallback** when connecting to backend nodes
- **TYPE command** to distinguish proxy nodes from end-servers in a tree topology
- Thread-safe **ConcurrentHashMap** for key-to-server routing
- Cached thread pools for scalable concurrent client handling
- Graceful **QUIT** propagation to all downstream nodes
- Startup validation of arguments with clear error messages

---

## Protocol

All messages are plain-text strings. TCP commands are newline-delimited (`\n`); UDP commands are sent as single datagrams (also appended with `\n` for server compatibility).

| Command | Description | Response |
|---|---|---|
| `GET NAMES` | List all keys known to this node | `OK <count> <key1> <key2> ...` |
| `GET VALUE <key>` | Get the value of a key | `OK <value>` or `NA` |
| `SET <key> <value>` | Set a key's value on the owning server | `OK <value>` or `NA` |
| `TYPE` | Identify this node's type | `PROXY` or `NA` |
| `QUIT` | Shut down this node (and all downstream nodes) | *(no response)* |

> **Extension:** The `TYPE` command is an addition to the base protocol. It allows a proxy to detect whether a configured peer is another proxy or an end-server during initialisation.

---

## Architecture

```
              ┌────────────────────────────────────┐
              │            Proxy Server            │
              │                                    │
  TCP Client ─┤  TCP Listener (ServerSocket)       │
  UDP Client ─┤  UDP Listener (DatagramSocket)     │
              │           │                        │
              │    przetworzProtokol()              │
              │    (key lookup in ConcurrentHashMap)│
              │           │                        │
              │  ┌─────────────────────────┐       │
              │  │  Key → RemoteServer map  │       │
              │  └─────────────────────────┘       │
              │           │                        │
              └───────────┼────────────────────────┘
                          │
            ┌─────────────┴──────────────┐
            │                            │
     TCP/UDP Server A            TCP/UDP Proxy B
                                         │
                                  TCP/UDP Server C
```

**Startup sequence:**
1. Parse CLI arguments (`-port`, `-server`)
2. For each configured node: send `TYPE` (TCP), then `GET NAMES` to populate the key map
3. If TCP fails, retry with UDP
4. Start UDP listener thread + TCP `ServerSocket` accept loop

---

## Usage

### Compile

```bash
javac Proxy.java
```

### Run

Please replace values between <>
```bash
java Proxy -port <port> -server <ip1> <port1> [-server <ip2> <port2> ...]
```

**Example — two backend servers:**

```bash
java Proxy -port 8080 -server 192.168.1.10 9000 -server 192.168.1.11 9001
```

**Example — chained proxies (tree topology):**

```bash
# Start leaf servers first, then intermediate proxies, then root proxy
java Proxy -port 8080 -server 127.0.0.1 8081 -server 127.0.0.1 8082
```

> **Note:** Always start backend/leaf servers before the proxy. The key map is built at startup and is not updated dynamically.

### Parameters

| Parameter | Description |
|---|---|
| `-port <n>` | Port this proxy listens on (TCP + UDP) |
| `-server <ip> <port>` | Backend node address; repeat for multiple nodes |

---

## Limitations

- The key-to-server map is **static** — it is built once at startup and not refreshed at runtime.
- **No cycle detection** in the proxy topology graph.
- Backend nodes must be running before the proxy starts.

---

## Tech Stack

- **Java** (standard library only — `java.net`, `java.io`, `java.util.concurrent`)
- No external dependencies

---
