import os
from pathlib import Path

NODE_A_IP = "16.16.213.121"
NODE_B_IP = "13.62.47.150"

CHUNK_PORT = 9001          # TCP — chunk transfer
HEARTBEAT_PORT = 9002      # UDP — heartbeat
DASHBOARD_PORT = 5000      # Flask dashboard

CHUNK_SIZE_BYTES = 512 * 1024   # 512 KB per chunk
BASE_DIR = Path(__file__).resolve().parent.parent
FASTA_FILE = str(BASE_DIR / "data" / "ecoli_k12.fasta")
HEARTBEAT_INTERVAL = 2          # seconds
HEARTBEAT_TIMEOUT = 3           # missed heartbeats before marking node dead
SOCKET_TIMEOUT = 10             # TCP socket timeout in seconds