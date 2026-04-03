from collections import deque
import time

# How many seconds without a heartbeat before a node is considered offline
# Heartbeat listener times out after 4 intervals; 3 misses = 12s total
_PEER_TIMEOUT_S = 12

def _std_dev(data) -> float:
    """Population standard deviation — used as jitter metric."""
    n = len(data)
    if n < 2:
        return 0.0
    mean = sum(data) / n
    variance = sum((x - mean) ** 2 for x in data) / n
    return variance ** 0.5

class MetricsStore:
    def __init__(self):
        # Node A metrics
        self.a_peer_rtts  = deque(maxlen=100)
        self.a_hb_rtts    = deque(maxlen=50)
        self.a_chunks_fetched = 0
        self.a_crc_fails  = 0
        self.a_peer_alive = None
        self._a_last_hb   = 0.0   # timestamp of last Node A heartbeat received

        # Node B metrics
        self.b_peer_rtts  = deque(maxlen=100)   # response times from peer_chunk_sent
        self.b_hb_rtts    = deque(maxlen=50)
        self.b_chunks_served = 0
        self.b_crc_fails  = 0
        self.b_peer_alive = None
        self._b_last_hb   = 0.0   # timestamp of last Node B heartbeat received

        # Client Sessions metrics
        self.active_clients = {}  # dict of client_id -> {'bytes_sent': 0, 'total': 0, 'ts': time}
        self.total_clients_served = 0
        self.total_bytes_served = 0
        self.client_serve_times = deque(maxlen=50)
        
        self.log = deque(maxlen=200)
        
    def record(self, e: dict):
        source = e.get("source_node", "A")
        self.log.append({**e, "ts": time.time()})
        t = e.get("type")
        now = time.time()

        # Ensure we have our chunk tracking timestamps safely initialized
        if not hasattr(self, '_a_last_chunk_ts'): self._a_last_chunk_ts = 0.0
        if not hasattr(self, '_b_last_chunk_ts'): self._b_last_chunk_ts = 0.0

        # NODE A METRICS (Coordinator)
        if source == "A":
            if t == "peer_fetch":
                self.a_peer_rtts.append(e["rtt_ms"])
                self._a_last_chunk_ts = now
                self.a_chunks_fetched += 1
                if not e.get("crc_ok", True):
                    self.a_crc_fails += 1
            elif t == "heartbeat":
                self.a_hb_rtts.append(e["rtt_ms"])
                self.a_peer_alive = True
                self._a_last_hb = time.time()
            elif t == "heartbeat_miss":
                self.a_peer_alive = e["missed"] < 3
        
        # NODE B METRICS (Peer Server)
        elif source == "B":
            if t == "peer_chunk_sent":
                self._b_last_chunk_ts = now
                self.b_chunks_served += 1
                # Use the actual measured response_time_ms if provided,
                # otherwise fall back to recording the chunk size as a proxy.
                if "response_time_ms" in e:
                    self.b_peer_rtts.append(e["response_time_ms"])
                elif "serve_time_ms" in e:
                    self.b_peer_rtts.append(e["serve_time_ms"])
            elif t == "heartbeat":
                self.b_hb_rtts.append(e["rtt_ms"])
                self.b_peer_alive = True
                self._b_last_hb = time.time()
            elif t == "heartbeat_miss":
                # Only mark offline if we also haven't heard from B recently
                self.b_peer_alive = e["missed"] < 3
                
        # CLIENT SESSIONS
        if t == "client_connected":
            self.active_clients[e["client_id"]] = {"bytes_sent": 0, "total": 0, "ts": time.time()}
        
        elif t == "client_progress":
            cid = e.get("client_id")
            if cid in self.active_clients:
                self.active_clients[cid]["bytes_sent"] = e["bytes_sent"]
                self.active_clients[cid]["total"] = e["total_bytes"]
                
        elif t == "client_served":
            cid = e.get("client_id")
            if cid in self.active_clients:
                del self.active_clients[cid]
            self.total_clients_served += 1
            self.total_bytes_served += e["total_bytes"]
            self.client_serve_times.append(e["fetch_time_s"])
            
        elif t == "client_disconnected":
            cid = e.get("client_id")
            if cid in self.active_clients:
                del self.active_clients[cid]

    def snapshot(self):
        now = time.time()
        
        # Clear stale chunk metrics so it falls back to live heartbeats
        if hasattr(self, '_a_last_chunk_ts') and (now - self._a_last_chunk_ts) > 2.0:
            self.a_peer_rtts.clear()
        if hasattr(self, '_b_last_chunk_ts') and (now - self._b_last_chunk_ts) > 2.0:
            self.b_peer_rtts.clear()

        # A stats — prefer chunk RTTs, fall back to heartbeat RTTs
        if self.a_peer_rtts:
            a_rtt    = round(sum(self.a_peer_rtts)/len(self.a_peer_rtts), 1)
            a_jitter = round(_std_dev(self.a_peer_rtts), 2) if len(self.a_peer_rtts) > 1 else None
        elif self.a_hb_rtts:
            a_rtt    = round(sum(self.a_hb_rtts)/len(self.a_hb_rtts), 1)
            a_jitter = round(_std_dev(self.a_hb_rtts), 2) if len(self.a_hb_rtts) > 1 else None
        else:
            a_rtt    = None
            a_jitter = None
            
        a_hb = round(sum(self.a_hb_rtts)/len(self.a_hb_rtts), 1) if self.a_hb_rtts else None
        # Mark A's peer offline if heartbeat is stale (even if no explicit miss event)
        if self._a_last_hb > 0 and (now - self._a_last_hb) > _PEER_TIMEOUT_S:
            self.a_peer_alive = False

        # B stats — use heartbeat RTT as the best available latency when chunk RTTs are absent
        if self.b_peer_rtts:
            b_rtt    = round(sum(self.b_peer_rtts)/len(self.b_peer_rtts), 1)
            b_jitter = round(_std_dev(self.b_peer_rtts), 2) if len(self.b_peer_rtts) > 1 else None
        elif self.b_hb_rtts:
            b_rtt    = round(sum(self.b_hb_rtts)/len(self.b_hb_rtts), 1)
            b_jitter = round(_std_dev(self.b_hb_rtts), 2) if len(self.b_hb_rtts) > 1 else None
        else:
            b_rtt    = None
            b_jitter = None
        b_hb = round(sum(self.b_hb_rtts)/len(self.b_hb_rtts), 1) if self.b_hb_rtts else None
        # Mark B's peer offline if heartbeat is stale
        if self._b_last_hb > 0 and (now - self._b_last_hb) > _PEER_TIMEOUT_S:
            self.b_peer_alive = False

        # Client stats
        avg_serve = round(sum(self.client_serve_times)/len(self.client_serve_times), 2) if self.client_serve_times else 0.00
        
        # Most recent client progress
        current_client_id = ""
        current_progress = 0
        if self.active_clients:
            # Pick the most recently updated or just the first
            current_client_id, stats = list(self.active_clients.items())[-1]
            if stats["total"] > 0:
                current_progress = int((stats["bytes_sent"] / stats["total"]) * 100)
                
        return {
            "node_a": {
                "self_alive": True,           # Node A hosts the dashboard — always up if serving
                "peer_rtt_avg": a_rtt,
                "jitter": a_jitter,
                "chunks_fetched": self.a_chunks_fetched,
                "crc_failures": self.a_crc_fails,
                "hb_rtt": a_hb,
                "peer_alive": self.a_peer_alive  # whether A can hear from B
            },
            "node_b": {
                "peer_rtt_avg": b_rtt,
                "jitter": b_jitter,
                "chunks_served": self.b_chunks_served,
                "crc_failures": self.b_crc_fails,
                "hb_rtt": b_hb,
                "peer_alive": self.b_peer_alive
            },
            "clients": {
                "active": len(self.active_clients),
                "total_served": self.total_clients_served,
                "mb_transferred": round(self.total_bytes_served / (1024*1024), 1),
                "avg_serve_time": avg_serve,
                "current_id": current_client_id,
                "current_progress": current_progress
            }
        }
