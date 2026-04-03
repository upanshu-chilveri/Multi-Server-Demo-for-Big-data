# node/metrics_forwarder.py
import socket, json
from common.config import NODE_A_IP

METRICS_UDP_PORT = 9004   # add this to config.py too

class MetricsForwarder:
    """Node B calls this instead of emitting locally.
       Serialises the event as JSON and sends it to Node A over UDP."""
    def __init__(self, my_role: str):
        self.role = my_role
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def emit(self, event: dict):
        event["source_node"] = self.role
        try:
            payload = json.dumps(event).encode()
            self._sock.sendto(payload, (NODE_A_IP, METRICS_UDP_PORT))
        except Exception as e:
            print(f"[MetricsForwarder] Failed to send: {e}")