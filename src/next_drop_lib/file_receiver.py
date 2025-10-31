import asyncio
import aiohttp.web
import os
import zstandard as zstd
from nercone_modern.logging import ModernLogging
from tqdm import tqdm

logger = ModernLogging("NextDrop")

class FileReceiver:
    def __init__(self, port, save_dir, compress=False):
        self.port = port
        self.save_dir = save_dir
        self.chunks = {}
        self.compress = compress
        self.lock = asyncio.Lock()
        self.filename = None
        self.total_chunks = None
        self.receive_bar = None

    async def handle_upload(self, request):
        if request.path == '/upload' and request.method == 'POST':
            try:
                chunk_number = int(request.query.get('chunk_number', -1))
                if chunk_number == -1:
                    return aiohttp.web.Response(status=400, text="ERROR: Invalid chunk number")
                
                version = request.headers.get('nextdp-version', '1.0')
                data = await request.read()

                if chunk_number == 0:
                    self.filename = request.headers.get('X-Filename', f"received_file_{int(asyncio.get_event_loop().time())}")
                    self.total_chunks = int(request.headers.get('X-Total-Chunks', '1'))
                    self.receive_bar = tqdm(total=self.total_chunks, desc="Receiving", unit="chunk")
                    logger.log(f"nextdp-version: {version}", "INFO")

                async with self.lock:
                    self.chunks[chunk_number] = data

                if self.receive_bar:
                    self.receive_bar.update(1)

                if self.total_chunks is not None and len(self.chunks) == self.total_chunks:
                    self.receive_bar.close()
                    asyncio.create_task(self.save_file(version))

                return aiohttp.web.Response(status=200, text="Chunk received successfully")
            except Exception as e:
                logger.log(f"Server error: {e}", "ERROR")
                return aiohttp.web.Response(status=500, text=f"Server error: {e}")
        logger.log(f"Unknown request path or method: {request.method} {request.path}", "WARNING")
        return aiohttp.web.Response(status=404, text="Not Found")

    async def start_server(self):
        app = aiohttp.web.Application(client_max_size=1024 * 1024 * 1024 * 5)
        app.router.add_post('/upload', self.handle_upload)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        logger.log(f"Receiver is listening on port {self.port}", "INFO")
        while True:
            await asyncio.sleep(3600)

    async def save_file(self, version):
        if not self.chunks:
            logger.log("No chunks received. Aborting save.", "WARNING")
            return

        sorted_chunks = sorted(self.chunks.items())
        file_data = b''.join([chunk for _, chunk in sorted_chunks])

        if self.compress:
            logger.log("Decompressing data.", "INFO")
            try:
                if version == "2.0":
                    dctx = zstd.ZstdDecompressor()
                    file_data = dctx.decompress(file_data)
                else:
                    import gzip
                    file_data = gzip.decompress(file_data)
            except Exception as e:
                logger.log("Failed to decompress. Please check the sender's compression settings.", "ERROR")
                return

        save_path = os.path.join(self.save_dir, self.filename)
        try:
            with open(save_path, 'wb') as f:
                f.write(file_data)
            logger.log(f"File '{self.filename}' saved to '{self.save_dir}'.", "INFO")

            self.chunks = {}
            self.filename = None
            self.total_chunks = None
            self.receive_bar = None

        except Exception as e:
            logger.log(f"Error while saving file: {e}", "ERROR")
