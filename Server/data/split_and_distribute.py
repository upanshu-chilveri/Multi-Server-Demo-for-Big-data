import os
from common.config import CHUNK_SIZE

with open("data/dataset.bin", "rb") as f:
    data = f.read()

chunks = [data[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
total = len(chunks)

os.makedirs("data/chunks_A", exist_ok=True)
os.makedirs("data/chunks_B", exist_ok=True)
for idx, chunk in enumerate(chunks):
    folder = "chunks_A" if idx % 2 == 0 else "chunks_B"
    with open(f"data/{folder}/chunk_{idx:04d}.bin", "wb") as f:
        f.write(chunk)

print(f"Total chunks: {total}")
print(f"Node A owns: even indices 0,2,4... ({(total+1)//2} chunks)")
print(f"Node B owns: odd indices 1,3,5... ({total//2} chunks)")
print("Distribution complete. Simulating distributed storage locally!")
