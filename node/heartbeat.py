import socket, struct, time, threading
from common.config import HEARTBEAT_PORT, HEARTBEAT_INTERVAL, NODE_A_IP, NODE_B_IP
from common.packet import pack, unpack, F_HEARTBEAT

class Heartbeat:
    def __init__(self, my_role: str, metrics_cb):
        # my_role is "A" or "B"
        self.peer_ip    = NODE_B_IP if my_role == "A" else NODE_A_IP
        self.metrics_cb = metrics_cb
        self.peer_alive = True
        self._seq       = 0

    def start(self):
        threading.Thread(target=self._sender,   daemon=True).start()
        threading.Thread(target=self._listener, daemon=True).start()

    def _sender(self):
        from common.config import THEIR_HEARTBEAT_PORT
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            ts  = time.time()
            pkt = pack(self._seq, 0, 0, struct.pack("!d", ts), F_HEARTBEAT)
            sock.sendto(pkt, (self.peer_ip, THEIR_HEARTBEAT_PORT))
            self._seq += 1
            time.sleep(HEARTBEAT_INTERVAL)

    def _listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", HEARTBEAT_PORT))
        sock.settimeout(HEARTBEAT_INTERVAL * 4)
        missed = 0
        while True:
            try:
                data, addr = sock.recvfrom(256)
                pkt   = unpack(data)
                sent  = struct.unpack("!d", pkt["payload"])[0]
                rtt   = (time.time() - sent) * 1000
                missed = 0
                self.peer_alive = True
                self.metrics_cb({"type": "heartbeat", "rtt_ms": round(rtt, 2), "peer": addr[0]})
            except socket.timeout:
                missed += 1
                self.peer_alive = missed < 3
                self.metrics_cb({"type": "heartbeat_miss", "missed": missed})
