import os, glob
from common.config import DATA_FILE, CHUNK_SIZE

class ChunkStore:
    def __init__(self, my_role: str):
        # my_role "A" -> even chunks, "B" -> odd chunks
        self.role   = my_role
        self.chunks = {}   # {chunk_id: bytes}
        self._load()

    def _load(self):
        # find the data dir relative to Server/ by resolving the folder above Server/node/
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(base_dir, "data", f"chunks_{self.role}")
        files = sorted(glob.glob(os.path.join(data_dir, "chunk_*.bin")))
        self.total_chunks = len(files)  # will be set authoritatively on first peer sync
        for path in files:
            idx = int(os.path.basename(path).split("_")[1].split(".")[0])
            with open(path, "rb") as f:
                self.chunks[idx] = f.read()
        print(f"[ChunkStore] Node {self.role} loaded {len(self.chunks)} chunks: {sorted(self.chunks.keys())}")

    def get(self, chunk_id: int) -> bytes | None:
        return self.chunks.get(chunk_id)

    def all_ids(self) -> list[int]:
        return sorted(self.chunks.keys())
