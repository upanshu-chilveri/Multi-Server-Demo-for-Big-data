import socket, threading, time
from common.config import PEER_PORT, SOCKET_TIMEOUT
from common.packet import pack, unpack, send_framed, recv_framed, F_DATA, F_ACK
from node.chunk_store import ChunkStore

class PeerServer:
    def __init__(self, store: ChunkStore, metrics_cb):
        self.store      = store
        self.metrics_cb = metrics_cb

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", PEER_PORT))
        srv.listen(5)
        print(f"[PeerServer] Listening on port {PEER_PORT}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def _handle(self, conn, addr):
        conn.settimeout(SOCKET_TIMEOUT)
        try:
            while True:
                raw = recv_framed(conn)
                req = unpack(raw)
                cid = req["chunk_id"]
                payload = self.store.get(cid)
                if payload is None:
                    payload = b""   # empty = chunk not here
                t0 = time.time()
                pkt = pack(req["seq"], cid, self.store.total_chunks, payload, F_DATA)
                send_framed(conn, pkt)
                serving_time = time.time() - t0
                self.metrics_cb({
                    "type": "peer_chunk_sent",
                    "chunk_id": cid,
                    "bytes": len(payload),
                    "to": addr[0],
                    "serving_time_ms": round(serving_time * 1000, 2),
                    "ts": time.time()
                })
        except (ConnectionError, TimeoutError):
            conn.close()
