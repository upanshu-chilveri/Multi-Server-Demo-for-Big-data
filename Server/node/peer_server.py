import socket, threading, time
from common.config import PEER_PORT, SOCKET_TIMEOUT
from common.packet import pack, unpack, send_framed, recv_framed, F_DATA, F_ACK
from node.chunk_store import ChunkStore

class PeerServer:
    def __init__(self, store: ChunkStore, metrics_cb, my_role: str = "A"):
        self.store      = store
        self.metrics_cb = metrics_cb
        self.my_role    = my_role

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
        mid_transfer = False   # True only while actively serving a chunk request
        try:
            while True:
                mid_transfer = False   # idle, waiting for next request
                t_recv = time.time()
                raw = recv_framed(conn)
                mid_transfer = True    # received a request, now processing
                req = unpack(raw)
                cid = req["chunk_id"]
                payload = self.store.get(cid)
                if payload is None:
                    payload = b""   # empty = chunk not here
                pkt = pack(req["seq"], cid, self.store.total_chunks, payload, F_DATA)
                send_framed(conn, pkt)
                mid_transfer = False   # successfully sent, back to idle
                # Measure time from receiving request to completing the send
                response_time_ms = round((time.time() - t_recv) * 1000, 2)
                self.metrics_cb({
                    "type": "peer_chunk_sent",
                    "source_node": self.my_role,
                    "chunk_id": cid,
                    "bytes": len(payload),
                    "to": addr[0],
                    "response_time_ms": response_time_ms,
                    "ts": t_recv
                })
        except (ConnectionError, TimeoutError, OSError) as e:
            # Only count as a drop if we were actively mid-transfer.
            # A clean close while idle (peer finished fetching) is normal.
            if mid_transfer:
                self.metrics_cb({
                    "type": "tcp_drop",
                    "source_node": self.my_role,
                    "peer": str(addr[0]),
                    "reason": str(e)
                })
            conn.close()
