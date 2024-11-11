# NextDrop - High-Speed Data Pipeline

import math
from KamuJpModern.ModernLogging import ModernLogging
import asyncio
import aiohttp
import aiohttp.web
import socket
import os
import sys
import gzip
import argparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from io import BytesIO

CHUNK_SIZE = 1024 * 1024  # 1MB
logger = ModernLogging("NextDrop")

class FileSender:
    def __init__(self, target, port, file_path, num_threads=4, compress=False):
        self.target = target
        self.port = port
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress

    async def send_file(self):
        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            total_chunks = 1
            logger.log("File size is 0. Setting total_chunks to 1.", "WARNING")
        else:
            total_chunks = math.ceil(file_size / CHUNK_SIZE)

        if self.compress:
            logger.log("Sending in compression mode.", "INFO")

        async with aiohttp.ClientSession() as session:
            tasks = []
            with open(self.file_path, 'rb') as f:
                compress_bar = tqdm(total=total_chunks, desc="Processing", unit="chunk")
                send_bar = tqdm(total=total_chunks, desc="Sending", unit="chunk")
                for i in range(total_chunks):
                    chunk = f.read(CHUNK_SIZE)
                    if self.compress:
                        with BytesIO() as buffer:
                            with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
                                gz.write(chunk)
                            compressed_chunk = buffer.getvalue()
                        data = compressed_chunk
                    else:
                        data = chunk
                    compress_bar.update(1)
                    task = asyncio.create_task(self.send_chunk(session, data, i, total_chunks if i == 0 else None, send_bar))
                    tasks.append(task)
            await asyncio.gather(*tasks)
            compress_bar.close()
            send_bar.close()

    async def send_chunk(self, session, chunk, chunk_number, total_chunks=None, send_bar=None):
        url = f'http://{self.target}:{self.port}/upload?chunk_number={chunk_number}'
        headers = {}
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

class FileReceiver:
    def __init__(self, port, save_dir, compress=False):
        self.port = port
        self.save_dir = save_dir
        self.chunks = {}
        self.compress = compress
        self.lock = asyncio.Lock()
        self.filename = None
        self.total_chunks = None
        self.recive_bar = None

    async def handle_upload(self, request):
        if request.path == '/upload' and request.method == 'POST':
            try:
                chunk_number = int(request.query.get('chunk_number', -1))
                if chunk_number == -1:
                    return aiohttp.web.Response(status=400, text="ERROR: Invalid chunk number")
                data = await request.read()

                # 最初のチャンクでファイル名と総チャンク数を取得
                if chunk_number == 0:
                    self.filename = request.headers.get('X-Filename', f"received_file_{int(asyncio.get_event_loop().time())}")
                    self.total_chunks = int(request.headers.get('X-Total-Chunks', '1'))
                    self.recive_bar = tqdm(total=self.total_chunks, desc="Receiving", unit="chunk")

                async with self.lock:
                    self.chunks[chunk_number] = data

                if self.recive_bar:
                    self.recive_bar.update(1)

                    if not self.recive_bar.n == (chunk_number + 1):
                        self.recive_bar.update((chunk_number + 1) - self.recive_bar.n)

                # すべてのチャンクを受信した場合、ファイルを保存
                if self.total_chunks is not None and len(self.chunks) == self.total_chunks:
                    self.recive_bar.close()
                    asyncio.create_task(self.save_file())

                return aiohttp.web.Response(status=200, text="Chunk received")
            except Exception as e:
                logger.log(f"Server error: {e}", "ERROR")
                return aiohttp.web.Response(status=500, text=f"Server error: {e}")
        logger.log(f"Unknown request path or method: {request.method} {request.path}", "WARNING")
        return aiohttp.web.Response(status=404, text="Not found")

    async def start_server(self):
        app = aiohttp.web.Application(client_max_size=1024 * 1024 * 1024 * 5)  # 最大5GBに設定
        app.router.add_post('/upload', self.handle_upload)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        logger.log(f"Receiver is listening on port {self.port}", "INFO")
        while True:
            await asyncio.sleep(3600)

    async def save_file(self):
        if not self.chunks:
            logger.log("No received chunks. Saving aborted.", "WARNING")
            return

        sorted_chunks = sorted(self.chunks.items())
        file_data = b''.join([chunk for _, chunk in sorted_chunks])

        if self.compress:
            logger.log("Decompressing data.", "INFO")
            try:
                file_data = gzip.decompress(file_data)
            except gzip.BadGzipFile:
                logger.log("Decompression failed. Check the compression setting on the sender side.", "ERROR")
                return

        save_path = os.path.join(self.save_dir, self.filename)

        try:
            with open(save_path, 'wb') as f:
                f.write(file_data)
            logger.log(f"File '{self.filename}' saved to '{self.save_dir}'.", "INFO")

            self.chunks = {}
            self.filename = None
            self.total_chunks = None
            self.recive_bar = None
            
        except Exception as e:
            logger.log(f"An error occurred while saving the file: {e}", "ERROR")

def get_local_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

async def main():
    parser = argparse.ArgumentParser(description="高速ファイル転送スクリプト")
    subparsers = parser.add_subparsers(dest='mode', help='モードを指定してください')

    # 送信モードの引数
    send_parser = subparsers.add_parser('send', help='ファイルを送信します')
    send_parser.add_argument('target', type=str, help='ターゲットのIPアドレスまたはローカルアドレス (.local で終わる)', default='127.0.0.1')
    send_parser.add_argument('--port', type=int, help='ターゲットのポート番号', default=4321)
    send_parser.add_argument('file_path', type=str, help='送信するファイルのパス')
    send_parser.add_argument('--threads', type=int, default=1, help='使用するスレッド数 (default: 1)')
    send_parser.add_argument('--compress', action='store_true', help='ファイルを圧縮して送信します')

    # 受信モードの引数
    receive_parser = subparsers.add_parser('receive', help='ファイルを受信します')
    receive_parser.add_argument('--port', type=int, help='受信するポート番号', default=4321)
    receive_parser.add_argument('save_dir', type=str, help='受信ファイルの保存先ディレクトリ')  # 'save_path' を 'save_dir' に変更
    receive_parser.add_argument('--compress', action='store_true', help='受信したファイルを解凍します')

    args = parser.parse_args()

    if args.mode == 'send':
        if not os.path.isfile(args.file_path):
            logger.log(f"Error: File '{args.file_path}' does not exist.", "ERROR")
            sys.exit(1)
        sender = FileSender(args.target, args.port, args.file_path, args.threads, args.compress)
        await sender.send_file()
        logger.log("File sending completed.", "INFO")
    elif args.mode == 'receive':
        if not os.path.isdir(args.save_dir):
            logger.log(f"Error: Directory '{args.save_dir}' does not exist.", "ERROR")
            sys.exit(1)
        receiver = FileReceiver(args.port, args.save_dir, args.compress)
        receiver_task = asyncio.create_task(receiver.start_server())
        await receiver_task
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    
    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        logger.log("Keyboard interrupt detected. Exiting...", "INFO")
        
