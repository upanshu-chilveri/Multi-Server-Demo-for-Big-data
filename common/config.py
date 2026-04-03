import sys

ROLE = "A"
if len(sys.argv) > 1 and sys.argv[1].upper() in ("A", "B"):
    ROLE = sys.argv[1].upper()

NODE_A_IP = "127.0.0.1" 
NODE_B_IP = "127.0.0.1" 

if ROLE == "A":
    CLIENT_PORT = 9001
    PEER_PORT = 9002
    HEARTBEAT_PORT = 9003
    DASHBOARD_PORT = 5001
    THEIR_PEER_PORT = 9012
    THEIR_HEARTBEAT_PORT = 9013
else:
    CLIENT_PORT = 9011
    PEER_PORT = 9012
    HEARTBEAT_PORT = 9013
    DASHBOARD_PORT = 5002
    THEIR_PEER_PORT = 9002
    THEIR_HEARTBEAT_PORT = 9003

CHUNK_SIZE    = 512 * 1024   # 512 KB per chunk
DATA_FILE     = "data/dataset.bin"
SOCKET_TIMEOUT = 10          # seconds
HEARTBEAT_INTERVAL = 2       # seconds between UDP pings
HEARTBEAT_MISS_LIMIT = 3     # missed pings before peer marked dead
