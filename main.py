# NextDrop - High-Speed Data Pipeline - Ice Memory Version (Fixed)

import math
import asyncio
import websockets
import socket
import os
import sys
import zstandard as zstd
import argparse
from concurrent.futures import ProcessPoolExecutor
import json
import aiofiles
import logging
import hashlib
import multiprocessing
import psutil
import tempfile
from KamuJpModern import KamuJpModern

# ロギングの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('NextDrop')

CHUNK_SIZE = 1 * 1024 * 1024  # 1MB
COMPRESSION_LEVEL = 3  # zstd compression level (1-22)

class FileSender:
    def __init__(self, target, port, file_path, compress=False, version="2.0"):
        self.target = target
        self.port = port
        self.file_path = file_path
        self.compress = compress
        self.version = version
        self.max_retries = 3  # 再試行回数
        self.chunk_hashes = {}
        self.executor = None
        self.initialize_executor()

    def initialize_executor(self):
        cpu_count = multiprocessing.cpu_count()
        self.executor = ProcessPoolExecutor(max_workers=cpu_count)
        logger.info(f"ProcessPoolExecutor initialized with {cpu_count} workers.")

    async def send_file(self):
        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            total_chunks = 1
            logger.warning("File size is 0. Setting total_chunks to 1.")
        else:
            total_chunks = math.ceil(file_size / CHUNK_SIZE)

        uri = f'ws://{self.target}:{self.port}/upload'
        async with websockets.connect(uri, max_size=None) as websocket:
            # ファイル全体のハッシュ値を計算
            file_hash = await self.calculate_file_hash()
            # メタデータの送信
            metadata = {
                'filename': os.path.basename(self.file_path),
                'total_chunks': total_chunks,
                'version': self.version,
                'compress': self.compress,
                'file_hash': file_hash
            }
            await websocket.send(json.dumps(metadata))
            logger.info(f"Sending file '{self.file_path}' to {self.target}:{self.port}")

            # チャンクの送信
            async with aiofiles.open(self.file_path, 'rb') as f:
                send_bar = KamuJpModern().modernProgressBar(total=total_chunks, process_name="Sending", process_color=32)
                send_bar.start()
                send_bar.notbusy()
                for chunk_number in range(total_chunks):
                    data = await f.read(CHUNK_SIZE)
                    if self.compress:
                        # 非同期圧縮
                        loop = asyncio.get_event_loop()
                        data = await loop.run_in_executor(self.executor, self.compress_data, data)
                    # チャンクのハッシュ値を計算
                    chunk_hash = hashlib.sha256(data).hexdigest()
                    self.chunk_hashes[chunk_number] = chunk_hash
                    # メッセージの作成
                    message = {
                        'chunk_number': chunk_number,
                        'data': data.decode('latin1'),  # バイナリデータを文字列に変換
                        'chunk_hash': chunk_hash
                    }
                    await self.send_chunk(websocket, message, send_bar)
                send_bar.finish()
            # チャンクハッシュの送信
            await websocket.send(json.dumps({'chunk_hashes': self.chunk_hashes}))
            logger.info("File transfer completed.")

    def compress_data(self, data):
        self.adjust_executor()
        compressor = zstd.ZstdCompressor(level=COMPRESSION_LEVEL)
        return compressor.compress(data)

    def adjust_executor(self):
        # システムのCPU負荷を取得
        cpu_usage = psutil.cpu_percent(interval=1)
        cpu_count = multiprocessing.cpu_count()

        # CPU負荷が高い場合、ワーカー数を減らす
        if cpu_usage > 80 and self.executor._max_workers > 1:
            new_worker_count = max(1, self.executor._max_workers - 1)
            self.executor._max_workers = new_worker_count
            logger.info(f"High CPU usage detected ({cpu_usage}%). Reducing worker count to {new_worker_count}.")
        # CPU負荷が低い場合、ワーカー数を増やす
        elif cpu_usage < 50 and self.executor._max_workers < cpu_count:
            new_worker_count = self.executor._max_workers + 1
            self.executor._max_workers = new_worker_count
            logger.info(f"Low CPU usage detected ({cpu_usage}%). Increasing worker count to {new_worker_count}.")

    async def calculate_file_hash(self):
        hasher = hashlib.sha256()
        async with aiofiles.open(self.file_path, 'rb') as f:
            while True:
                data = await f.read(CHUNK_SIZE)
                if not data:
                    break
                hasher.update(data)
        return hasher.hexdigest()

    async def send_chunk(self, websocket, message, send_bar):
        for attempt in range(1, self.max_retries + 1):
            try:
                await websocket.send(json.dumps(message))
                # ACKの受信
                ack = await websocket.recv()
                ack = json.loads(ack)
                if ack.get('status') == 'ok':
                    send_bar.update()
                    break
                else:
                    raise Exception(f"Receiver reported error: {ack.get('error')}")
            except Exception as e:
                logger.error(f"Error sending chunk {message['chunk_number']} (Attempt {attempt}): {e}")
                if attempt == self.max_retries:
                    logger.error(f"Failed to send chunk {message['chunk_number']} after {self.max_retries} attempts.")
                    sys.exit(1)
                await asyncio.sleep(1)  # 再試行前に待機

class FileReceiver:
    def __init__(self, port, save_dir):
        self.port = port
        self.save_dir = save_dir
        self.filename = None
        self.total_chunks = None
        self.compress = False
        self.file_hash = None
        self.chunk_hashes = {}
        self.max_retries = 3  # 再試行回数
        self.executor = None
        self.initialize_executor()
        self.temp_dir = tempfile.mkdtemp()  # 一時ディレクトリの作成

    def initialize_executor(self):
        cpu_count = multiprocessing.cpu_count()
        self.executor = ProcessPoolExecutor(max_workers=cpu_count)
        logger.info(f"ProcessPoolExecutor initialized with {cpu_count} workers.")

    def adjust_executor(self):
        # システムのCPU負荷を取得
        cpu_usage = psutil.cpu_percent(interval=1)
        cpu_count = multiprocessing.cpu_count()

        # CPU負荷が高い場合、ワーカー数を減らす
        if cpu_usage > 80 and self.executor._max_workers > 1:
            new_worker_count = max(1, self.executor._max_workers - 1)
            self.executor._max_workers = new_worker_count
            logger.info(f"High CPU usage detected ({cpu_usage}%). Reducing worker count to {new_worker_count}.")
        # CPU負荷が低い場合、ワーカー数を増やす
        elif cpu_usage < 50 and self.executor._max_workers < cpu_count:
            new_worker_count = self.executor._max_workers + 1
            self.executor._max_workers = new_worker_count
            logger.info(f"Low CPU usage detected ({cpu_usage}%). Increasing worker count to {new_worker_count}.")

    async def start_server(self):
        async def handler(websocket):
            # メタデータの受信
            metadata_str = await websocket.recv()
            metadata = json.loads(metadata_str)
            self.filename = metadata['filename']
            self.total_chunks = metadata['total_chunks']
            self.compress = metadata['compress']
            self.file_hash = metadata['file_hash']
            logger.info(f"Receiving file '{self.filename}' with {self.total_chunks} chunks.")

            receive_bar = KamuJpModern().modernProgressBar(total=self.total_chunks, process_name="Receiving", process_color=32)
            receive_bar.start()
            receive_bar.notbusy()
            # チャンクの受信
            for _ in range(self.total_chunks):
                for attempt in range(1, self.max_retries + 1):
                    try:
                        message_str = await websocket.recv()
                        message = json.loads(message_str)
                        chunk_number = message['chunk_number']
                        data = message['data'].encode('latin1')  # 文字列をバイナリデータに変換
                        chunk_hash = message['chunk_hash']
                        # チャンクのハッシュ値を検証
                        if hashlib.sha256(data).hexdigest() != chunk_hash:
                            raise Exception("Chunk hash mismatch.")
                        self.chunk_hashes[chunk_number] = chunk_hash
                        # チャンクを一時ファイルに保存
                        await self.save_chunk_to_tempfile(chunk_number, data)
                        # ACKの送信
                        await websocket.send(json.dumps({'status': 'ok'}))
                        receive_bar.update()
                        break
                    except Exception as e:
                        logger.error(f"Error receiving chunk {chunk_number} (Attempt {attempt}): {e}")
                        if attempt == self.max_retries:
                            logger.error(f"Failed to receive chunk {chunk_number} after {self.max_retries} attempts.")
                            await websocket.send(json.dumps({'status': 'error', 'error': str(e)}))
                            sys.exit(1)
                        await asyncio.sleep(1)  # 再試行前に待機
            receive_bar.finish()

            # チャンクハッシュの受信
            chunk_hashes_str = await websocket.recv()
            received_chunk_hashes = json.loads(chunk_hashes_str).get('chunk_hashes', {})
            # チャンクハッシュの検証
            if self.chunk_hashes != received_chunk_hashes:
                logger.error("Chunk hashes do not match.")
                sys.exit(1)

            # ファイルの保存
            await self.save_file()
            logger.info("File received and saved successfully.")

        server = await websockets.serve(handler, '0.0.0.0', self.port, max_size=None)
        logger.info(f"Receiver is listening on port {self.port}")
        await server.wait_closed()

    async def save_chunk_to_tempfile(self, chunk_number, data):
        temp_file_path = os.path.join(self.temp_dir, f"chunk_{chunk_number}")
        # 非同期ファイル書き込み
        async with aiofiles.open(temp_file_path, 'wb') as temp_file:
            await temp_file.write(data)
        # メモリ上のデータを削除
        del data

    async def save_file(self):
        save_path = os.path.join(self.save_dir, self.filename)
        # ファイルの書き込み
        async with aiofiles.open(save_path, 'wb') as final_file:
            for chunk_number in range(self.total_chunks):
                temp_file_path = os.path.join(self.temp_dir, f"chunk_{chunk_number}")
                async with aiofiles.open(temp_file_path, 'rb') as temp_file:
                    data = await temp_file.read()
                    if self.compress:
                        # 非同期解凍
                        loop = asyncio.get_event_loop()
                        data = await loop.run_in_executor(self.executor, self.decompress_data, data)
                    await final_file.write(data)
                # 一時ファイルを削除してメモリを解放
                os.remove(temp_file_path)
        # 一時ディレクトリを削除
        os.rmdir(self.temp_dir)
        # ファイルのハッシュ値を検証
        if await self.calculate_file_hash(save_path) != self.file_hash:
            logger.error("File hash does not match.")
            sys.exit(1)

    def decompress_data(self, data):
        self.adjust_executor()
        decompressor = zstd.ZstdDecompressor()
        return decompressor.decompress(data)

    async def calculate_file_hash(self, file_path):
        hasher = hashlib.sha256()
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                data = await f.read(CHUNK_SIZE)
                if not data:
                    break
                hasher.update(data)
        return hasher.hexdigest()

def get_local_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

async def main():
    parser = argparse.ArgumentParser(description="High-Speed File Transfer Script")
    subparsers = parser.add_subparsers(dest='mode', help='Specify the mode')

    # Send mode arguments
    send_parser = subparsers.add_parser('send', help='Send a file')
    send_parser.add_argument('target', type=str, help='Target IP address or local address', default='127.0.0.1')
    send_parser.add_argument('--port', type=int, help='Target port number', default=4321)
    send_parser.add_argument('file_path', type=str, help='Path of the file to send')
    send_parser.add_argument('--compress', action='store_true', help='Compress the file before sending')
    send_parser.add_argument('--version', type=str, default="2.0", help='Version of the nextdp protocol')

    # Receive mode arguments
    receive_parser = subparsers.add_parser('receive', help='Receive a file')
    receive_parser.add_argument('--port', type=int, help='Port number to receive on', default=4321)
    receive_parser.add_argument('save_dir', type=str, help='Directory to save the received file')

    args = parser.parse_args()

    if args.mode == 'send':
        if not os.path.isfile(args.file_path):
            logger.error(f"Error: File '{args.file_path}' does not exist.")
            sys.exit(1)
        sender = FileSender(args.target, args.port, args.file_path, args.compress, args.version)
        await sender.send_file()
    elif args.mode == 'receive':
        if not os.path.isdir(args.save_dir):
            logger.error(f"Error: Directory '{args.save_dir}' does not exist.")
            sys.exit(1)
        receiver = FileReceiver(args.port, args.save_dir)
        await receiver.start_server()
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Exiting...")
