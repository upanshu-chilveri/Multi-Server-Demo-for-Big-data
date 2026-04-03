import os, random, string

size_bytes = 50 * 1024 * 1024  # 50 MB
print(f"Generating {size_bytes // (1024*1024)} MB dataset...")
os.makedirs("data", exist_ok=True)
with open("data/dataset.bin", "wb") as f:
    f.write(os.urandom(size_bytes))
print("Done: data/dataset.bin")
