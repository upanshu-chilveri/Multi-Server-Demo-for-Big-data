import socket, time
from common.config import PEER_PORT, SOCKET_TIMEOUT
from common.packet import pack, unpack, send_framed, recv_framed, F_PEER_REQ
from node.chunk_store import ChunkStore

class Coordinator:
    def __init__(self, store: ChunkStore, peer_ip: str, metrics_cb):
        self.store      = store
        self.peer_ip    = peer_ip
        self.metrics_cb = metrics_cb

    def fetch_full_file(self) -> bytes:
        """Collect all chunks from local store + peer, merge in order."""
        all_chunks = dict(self.store.chunks)   # start with local chunks
        peer_ids   = self._peer_chunk_ids()

        from common.config import THEIR_PEER_PORT
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.settimeout(SOCKET_TIMEOUT)
        peer_sock.connect((self.peer_ip, THEIR_PEER_PORT))

        seq = 1000
        for cid in peer_ids:
            req = pack(seq, cid, 0, b"", F_PEER_REQ)
            t0  = time.time()
            send_framed(peer_sock, req)
            raw = recv_framed(peer_sock)
            rtt = (time.time() - t0) * 1000
            pkt = unpack(raw)

            all_chunks[pkt["chunk_id"]] = pkt["payload"]
            self.metrics_cb({
                "type": "peer_fetch",
                "chunk_id": pkt["chunk_id"],
                "bytes": pkt["payload_len"],
                "rtt_ms": round(rtt, 2),
                "crc_ok": pkt["crc_ok"],
                "ts": time.time()
            })
            seq += 1

        peer_sock.close()

        # Merge in chunk_id order
        total = max(all_chunks.keys()) + 1
        missing = [i for i in range(total) if i not in all_chunks]
        if missing:
            print(f"[Coordinator] WARNING: missing chunks {missing}")

        merged = b"".join(all_chunks[i] for i in range(total) if i in all_chunks)
        print(f"[Coordinator] Merged {len(all_chunks)} chunks -> {len(merged)} bytes")
        return merged

    def _peer_chunk_ids(self) -> list[int]:
        """Return the chunk IDs the peer holds (opposite parity to us)."""
        my_ids  = set(self.store.all_ids())
        # We know total from the store - peer holds all IDs we don't
        total   = self.store.total_chunks
        return [i for i in range(total) if i not in my_ids]
