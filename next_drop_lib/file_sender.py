import math
import asyncio
import aiohttp
import os
import zstandard as zstd  # Changed from gzip to zstandard
from KamuJpModern.ModernLogging import ModernLogging
from tqdm import tqdm

CHUNK_SIZE = 1024 * 1024  # 1MB
DEFAULT_MAX_CONCURRENT_TASKS = 100  # Maximum number of concurrent tasks
COMPRESSION_LEVEL = 3  # zstd compression level (1-22)
logger = ModernLogging("NextDrop")

class FileSender:
    def __init__(self, target, port, file_path, num_threads=4, compress=False, version="2.0"):
        self.target = target
        self.port = port
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress
        self.version = version
        self.semaphore = asyncio.Semaphore(DEFAULT_MAX_CONCURRENT_TASKS)
        self.zstd_compressor = zstd.ZstdCompressor(level=COMPRESSION_LEVEL) if self.compress else None

    async def send_file(self):
        file_size = os.path.getsize(self.file_path)
        total_chunks = max(1, math.ceil(file_size / CHUNK_SIZE))

        if self.compress:
            logger.log("Sending in compression mode.", "INFO")

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=None)) as session:
            tasks = []
            with open(self.file_path, 'rb') as f:
                compress_bar = tqdm(total=total_chunks, desc="Processing", unit="chunk")
                send_bar = tqdm(total=total_chunks, desc="Sending", unit="chunk")
                for i in range(total_chunks):
                    chunk = f.read(CHUNK_SIZE)
                    data = self.zstd_compressor.compress(chunk) if self.compress else chunk
                    compress_bar.update(1)
                    await self.semaphore.acquire()
                    task = asyncio.create_task(self.send_chunk(session, data, i, total_chunks if i == 0 else None, send_bar))
                    task.add_done_callback(lambda t: self.semaphore.release())
                    tasks.append(task)
            await asyncio.gather(*tasks)
            compress_bar.close()
            send_bar.close()

    async def send_chunk(self, session, chunk, chunk_number, total_chunks=None, send_bar=None):
        url = f'http://{self.target}:{self.port}/upload?chunk_number={chunk_number}'
        headers = {
            'nextdp-version': self.version
        }
        if chunk_number == 0:
            headers['X-Filename'] = os.path.basename(self.file_path)
            headers['X-Total-Chunks'] = str(total_chunks)
        try:
            async with session.post(url, data=chunk, headers=headers) as resp:
                if resp.status != 200:
                    logger.log(f"Failed to send chunk {chunk_number}: Status {resp.status}", "ERROR")
                else:
                    if send_bar:
                        send_bar.update(1)
        except Exception as e:
            logger.log(f"Exception occurred while sending chunk {chunk_number}: {e}", "ERROR")
