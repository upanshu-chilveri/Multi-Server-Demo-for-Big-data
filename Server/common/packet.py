import struct, zlib, time

# Header: seq_num(4) chunk_id(4) total_chunks(4) payload_len(4) checksum(4) flags(2) = 22 bytes
HEADER_FMT  = "!IIIII H"
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 22

# Flags
F_DATA        = 0b0000_0001   # regular data packet
F_ACK         = 0b0000_0010   # acknowledgement
F_FIN         = 0b0000_0100   # last chunk in stream
F_RETRANSMIT  = 0b0000_1000   # this is a retry
F_PEER_REQ    = 0b0001_0000   # node-to-node fetch request
F_HEARTBEAT   = 0b0010_0000   # UDP heartbeat ping

def pack(seq: int, chunk_id: int, total: int, payload: bytes, flags: int = F_DATA) -> bytes:
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    hdr = struct.pack(HEADER_FMT, seq, chunk_id, total, len(payload), crc, flags)
    return hdr + payload

def unpack(raw: bytes) -> dict:
    if len(raw) < HEADER_SIZE:
        raise ValueError(f"Too short: {len(raw)} bytes")
    seq, chunk_id, total, plen, crc, flags = struct.unpack(HEADER_FMT, raw[:HEADER_SIZE])
    payload = raw[HEADER_SIZE: HEADER_SIZE + plen]
    crc_ok  = (zlib.crc32(payload) & 0xFFFFFFFF) == crc
    return {
        "seq": seq, "chunk_id": chunk_id, "total": total,
        "payload_len": plen, "crc_ok": crc_ok,
        "flags": flags, "payload": payload,
        "recv_ts": time.time()
    }

def send_framed(sock, pkt: bytes):
    """Prefix every message with a 4-byte length so receiver knows how much to read."""
    sock.sendall(struct.pack("!I", len(pkt)) + pkt)

def recv_framed(sock) -> bytes:
    raw = _recv_exact(sock, 4)
    n   = struct.unpack("!I", raw)[0]
    return _recv_exact(sock, n)

def _recv_exact(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed mid-read")
        buf += chunk
    return buf
