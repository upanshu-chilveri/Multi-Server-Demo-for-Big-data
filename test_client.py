import socket
from common.packet import recv_framed, unpack, F_FIN
from common.config import NODE_A_IP, CLIENT_PORT

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((NODE_A_IP, CLIENT_PORT))
print("Connected. Requesting full file...")

pieces = []
while True:
    raw = recv_framed(sock)
    pkt = unpack(raw)
    pieces.append(pkt["payload"])
    if pkt["flags"] & F_FIN:
        break

data = b"".join(pieces)
with open("received_file.bin", "wb") as f:
    f.write(data)
print(f"Received {len(data)} bytes -> saved as received_file.bin")
sock.close()
