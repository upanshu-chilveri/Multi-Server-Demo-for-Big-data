import sys, threading
from common.config import NODE_A_IP, NODE_B_IP
from node.chunk_store  import ChunkStore
from node.peer_server  import PeerServer
from node.coordinator  import Coordinator
from node.client_server import ClientServer
from node.heartbeat    import Heartbeat
from dashboard.app     import start_dashboard, metrics_cb

# Expecting arguments like "main.py A 20"
try:
    role = sys.argv[1].upper()   # "A" or "B"
    total_chunk_count = int(sys.argv[2])
except (IndexError, ValueError):
    print("Usage: python3 main.py <A|B> <total_chunks>")
    sys.exit(1)

peer_ip = NODE_B_IP if role == "A" else NODE_A_IP

store  = ChunkStore(role)
store.total_chunks = total_chunk_count   # pass total chunk count at launch

hb     = Heartbeat(role, metrics_cb)
hb.start()

peer_srv  = PeerServer(store, metrics_cb)
peer_srv.start()

coord     = Coordinator(store, peer_ip, metrics_cb)
client_srv = ClientServer(coord, metrics_cb)
client_srv.start()

start_dashboard()   # blocks - keep last
