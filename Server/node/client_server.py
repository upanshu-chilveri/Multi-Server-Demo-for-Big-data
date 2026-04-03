import socket, threading, time
from common.config import CLIENT_PORT, SOCKET_TIMEOUT
from common.packet import pack, send_framed, F_DATA, F_FIN
from node.coordinator import Coordinator

class ClientServer:
    def __init__(self, coordinator: Coordinator, metrics_cb):
        self.coord      = coordinator
        self.metrics_cb = metrics_cb

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", CLIENT_PORT))
        srv.listen(5)
        print(f"[ClientServer] Listening on port {CLIENT_PORT}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def _handle(self, conn, addr):
        client_id = f"client-{addr[1]}"  # Use port to distinguish clients
        print(f"[ClientServer] Client connected: {client_id} at {addr}")
        conn.settimeout(SOCKET_TIMEOUT)
        
        self.metrics_cb({
            "type": "client_connected",
            "client_id": client_id
        })
        
        try:
            t0     = time.time()
            data   = self.coord.fetch_full_file()

            # Stream merged file back to client as framed packets
            if len(data) == 0:
                pkt = pack(0, 0, 0, b"", F_FIN)
                send_framed(conn, pkt)
            else:
                chunk_size = 64 * 1024   # 64 KB send window
                total_sent = 0
                seq = 0
                for i in range(0, len(data), chunk_size):
                    chunk   = data[i:i+chunk_size]
                    is_last = (i + chunk_size >= len(data))
                    flags   = F_FIN if is_last else F_DATA
                    pkt     = pack(seq, 0, 0, chunk, flags)
                    send_framed(conn, pkt)
                    total_sent += len(chunk)
                    seq += 1
                    
                    # Emit progress periodically to avoid flooding
                    if seq % 20 == 0 or is_last:
                        self.metrics_cb({
                            "type": "client_progress",
                            "client_id": client_id,
                            "bytes_sent": total_sent,
                            "total_bytes": len(data)
                        })

            # Calculate total time after the entire file has been transmitted to the client
            fetch_time = max(time.time() - t0, 0.001)  # avoid ZeroDivisionError
            throughput = len(data) / fetch_time / 1024
            self.metrics_cb({
                "type": "client_served",
                "client_id": client_id,
                "total_bytes": len(data),
                "fetch_time_s": round(fetch_time, 3),
                "throughput_kbps": round(throughput, 1),
                "client": addr[0]
            })
            print(f"[ClientServer] Sent {total_sent} bytes to {addr} in {fetch_time:.2f}s")
        except (ConnectionError, TimeoutError, OSError) as e:
            print(f"[ClientServer] Network drop: {e}")
            self.metrics_cb({
                "type": "tcp_drop",
                "source_node": "A",
                "peer": str(addr[0]),
                "reason": str(e)
            })
            self.metrics_cb({"type": "client_disconnected", "client_id": client_id})
        except Exception as e:
            print(f"[ClientServer] Unexpected error: {e}")
            self.metrics_cb({"type": "client_disconnected", "client_id": client_id})
        finally:
            conn.close()
