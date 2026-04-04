# Distributed Database Simulator

A highly resilient, high-performance distributed database simulator and telemetry system written in Python. This project demonstrates core distributed systems concepts including data chunking, real-time node synchronization, custom TCP/UDP wire protocols, and active telemetry monitoring.

## 🚀 Features

- **Distributed Data Storage**: A large binary dataset is striped across two nodes (Node A and Node B).
- **Custom Wire Protocol**: Data is framed and transmitted over TCP/UDP utilizing a custom binary protocol header to ensure efficient decoding, validation, and minimal overhead.
- **Data Integrity & Resiliency**: Built-in `zlib` CRC32 checksums drop and retry corrupted chunk data up to 3 times before timing out, guarding against in-flight data corruption.
- **Node Sync & Coordinator**: Node A acts as the cluster Coordinator. When a client requests the full file, Node A dynamically queries Node B for the chunks it lacks, merges them sequentially in-memory, and streams the finished file directly to the client.
- **High-Frequency Telemetry**: Both nodes utilize UDP heartbeat listeners and active TCP dropping counters to determine link stability, calculating Round-Trip Time (RTT) and Jitter (Population Standard Deviation) natively.
- **Real-Time Monitoring Dashboard**: A Flask & Socket.IO web application visualizing cluster health asynchronously, tracking peer statuses, chunk counts, CRC failures, and client download progressions.

<!-- Image Generation Prompt:
"A high-tech cloud architecture diagram showing two server nodes, Node A and Node B, connected via dual TCP and UDP data pipes. Node A has a glowing 'Coordinator' badge and connects to an external 'Client' computer. A sleek 'Dashboard' screen floats above Node A prominently displaying metric line graphs. The background should be a dark, futuristic tech blueprint aesthetic."
-->

## 🧠 Core Concepts

### 1. Data Chunking & Striping
To simulate a multi-node database, massive files (e.g., `dataset.bin`) are split into standard 512KB chunks. Node A stores the even-indexed chunks while Node B stores the odd-indexed chunks. To reconstruct the dataset successfully, the cluster must sync and coordinate transparently.

### 2. The Custom Packet Protocol
Rather than utilizing heavy HTTP overhead, the system defines a custom byte-packed header mechanism using Python's `struct` library:
- **Header Structure**: `Sequence Number`, `Chunk ID`, `Total Chunks`, `Payload Length`, `Checksum`, `Flags`.
- Using raw TCP streams alongside structured framing completely avoids stream desynchronization and maximizes bandwidth utility.

### 3. Asynchronous Heartbeats
Node isolation and connectivity drops are detected in real-time. UDP heartbeats are blasted every second across the cluster. If 3 consecutive intervals pass without a pulse, the link is immediately marked as dead, and the dashboard flashes offline.

### 4. Telemetry Fallbacks
If a node stops querying chunks, RTT stops updating since no data is flowing. To prevent frozen metrics, the system seamlessly "falls back" to displaying UDP heartbeat transit times, ensuring network latency is robustly monitored even during idle periods.

<!-- Image Generation Prompt:
"A glowing, dark-themed dashboard UI showing real-time server metrics. The layout has three sleek translucent panels with glassmorphism and blur effects. The panels display 'Node A', 'Node B', and 'Client Sessions'. Inside the panels are glowing digital numbers showing 'Peer RTT avg', 'Jitter', CRC failures, and glowing progress bars. The color palette focuses on deep neon blues, purples, and vibrant green 'Online' status indicators."
-->

## 🛠️ Project Structure
* `Server/main.py`: The main entry point to start the cluster nodes.
* `Server/node/`: Contains the distributed logic engine (`coordinator.py`, `peer_server.py`, `client_server.py`, `chunk_store.py`).
* `Server/common/`: Shared cluster configuration and custom packet manipulation structure.
* `Server/dashboard/`: The Flask & Socket.IO frontend utilized to display telemetry UI templates.
* `Client.py`: The isolated test client script utilized to execute a stress-test block transfer via TCP.

## ⚙️ Running the Cluster

### Generate the data
```bash
python data/generate.py
```
*Generates the data to be retrieved.*
### Split and distribute the data
```bash
python3 -m  data.split_and_distribute
```
*splits the data into chunks and distributes it betweeen nodes*
### Start Node A (Coordinator & Dashboard)
```bash
python Server/main.py A 100
```
*Starts the dashboard on `http://localhost:5001` and natively listens for Node B.*

### Start Node B (Peer Data Store)
```bash
python Server/main.py B 100
```

### Run a Client Fetch Transfer
```bash
python Client.py
```
*Initiates a full-file fetch. The dashboard will instantly visualize the chunk aggregation, track network drops, and intelligently calculate actual download throughput.*
