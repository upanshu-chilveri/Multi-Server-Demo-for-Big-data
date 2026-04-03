# main.py — updated
import sys, threading
from common.config import NODE_A_IP, NODE_B_IP
from node.chunk_store   import ChunkStore
from node.peer_server   import PeerServer
from node.coordinator   import Coordinator
from node.client_server import ClientServer
from node.heartbeat     import Heartbeat
from node.metrics_forwarder import MetricsForwarder

role    = sys.argv[1].upper()
total   = int(sys.argv[2])
peer_ip = NODE_B_IP if role == "A" else NODE_A_IP

if role == "A":
    # Node A runs the dashboard and emits locally
    from dashboard.app import metrics_cb, start_dashboard
else:
    # Node B forwards all events to Node A's dashboard over UDP
    fwd = MetricsForwarder("B")
    metrics_cb = fwd.emit

store = ChunkStore(role)
store.total_chunks = total

hb        = Heartbeat(role, metrics_cb)
hb.start()

peer_srv  = PeerServer(store, metrics_cb)
peer_srv.start()

coord     = Coordinator(store, peer_ip, metrics_cb)
client_srv = ClientServer(coord, metrics_cb)
client_srv.start()

if role == "A":
    start_dashboard()   # blocks — only Node A runs the dashboard
else:
    print("[Node B] Running. Metrics forwarding to Node A dashboard.")
    import time
    while True:
        time.sleep(60)