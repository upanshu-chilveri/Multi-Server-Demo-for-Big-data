from collections import deque
import time

class MetricsStore:
    def __init__(self):
        self.peer_rtts       = deque(maxlen=100)
        self.heartbeat_rtts  = deque(maxlen=50)
        self.chunks_fetched  = 0
        self.crc_failures    = 0
        self.bytes_served    = 0
        self.client_requests = 0
        self.peer_alive      = True
        self.log             = deque(maxlen=200)

    def record(self, e: dict):
        self.log.append({**e, "ts": time.time()})
        t = e.get("type")
        if t == "peer_fetch":
            self.peer_rtts.append(e["rtt_ms"])
            self.chunks_fetched += 1
            if not e.get("crc_ok", True):
                self.crc_failures += 1
        elif t == "heartbeat":
            self.heartbeat_rtts.append(e["rtt_ms"])
            self.peer_alive = True
        elif t == "heartbeat_miss":
            self.peer_alive = e["missed"] < 3
        elif t == "client_served":
            self.bytes_served    += e["total_bytes"]
            self.client_requests += 1

    def snapshot(self):
        pr = list(self.peer_rtts)
        hb = list(self.heartbeat_rtts)
        return {
            "peer_rtt_avg":   round(sum(pr)/len(pr), 2) if pr else 0,
            "peer_rtt_hist":  pr[-30:],
            "hb_rtt_avg":     round(sum(hb)/len(hb), 2) if hb else 0,
            "chunks_fetched": self.chunks_fetched,
            "crc_failures":   self.crc_failures,
            "bytes_served":   self.bytes_served,
            "client_requests":self.client_requests,
            "peer_alive":     self.peer_alive,
            "jitter_ms":      round(max(pr)-min(pr), 2) if len(pr)>1 else 0,
        }
