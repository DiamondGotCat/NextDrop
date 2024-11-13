# NextDrop - High-Speed Data Pipeline - Final Optimized Version

import math
import asyncio
import websockets
import socket
import os
import sys
import zstandard as zstd
import argparse
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import json
import aiofiles
import logging
import hashlib

# ロギングの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('NextDrop')

CHUNK_SIZE = 50 * 1024 * 1024  # 50MB
COMPRESSION_LEVEL = 3  # zstd compression level (1-22)

class FileSender:
    def __init__(self, target, port, file_path, compress=False, version="2.0"):
        self.target = target
        self.port = port
        self.file_path = file_path
        self.compress = compress
        self.version = version
        self.executor = ProcessPoolExecutor()
        self.chunk_hashes = {}
        self.max_retries = 3  # 再試行回数

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
                send_bar = tqdm(total=total_chunks, desc="Sending", unit="chunk")
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
                send_bar.close()
            # チャンクハッシュの送信
            await websocket.send(json.dumps({'chunk_hashes': self.chunk_hashes}))
            logger.info("File transfer completed.")

    def compress_data(self, data):
        compressor = zstd.ZstdCompressor(level=COMPRESSION_LEVEL)
        return compressor.compress(data)

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
                    send_bar.update(1)
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
        self.chunks = {}
        self.filename = None
        self.total_chunks = None
        self.compress = False
        self.file_hash = None
        self.executor = ProcessPoolExecutor()
        self.chunk_hashes = {}
        self.max_retries = 3  # 再試行回数

    async def start_server(self):
        async def handler(websocket, path):
            # メタデータの受信
            metadata_str = await websocket.recv()
            metadata = json.loads(metadata_str)
            self.filename = metadata['filename']
            self.total_chunks = metadata['total_chunks']
            self.compress = metadata['compress']
            self.file_hash = metadata['file_hash']
            logger.info(f"Receiving file '{self.filename}' with {self.total_chunks} chunks.")

            receive_bar = tqdm(total=self.total_chunks, desc="Receiving", unit="chunk")
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
                        self.chunks[chunk_number] = data
                        self.chunk_hashes[chunk_number] = chunk_hash
                        # ACKの送信
                        await websocket.send(json.dumps({'status': 'ok'}))
                        receive_bar.update(1)
                        break
                    except Exception as e:
                        logger.error(f"Error receiving chunk {chunk_number} (Attempt {attempt}): {e}")
                        if attempt == self.max_retries:
                            logger.error(f"Failed to receive chunk {chunk_number} after {self.max_retries} attempts.")
                            await websocket.send(json.dumps({'status': 'error', 'error': str(e)}))
                            sys.exit(1)
                        await asyncio.sleep(1)  # 再試行前に待機
            receive_bar.close()

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

    async def save_file(self):
        save_path = os.path.join(self.save_dir, self.filename)
        # チャンクを順序通りに取得
        sorted_chunks = [self.chunks[i] for i in sorted(self.chunks.keys())]

        # ファイルの書き込み
        async with aiofiles.open(save_path, 'wb') as f:
            for chunk in sorted_chunks:
                if self.compress:
                    # 非同期解凍
                    loop = asyncio.get_event_loop()
                    chunk = await loop.run_in_executor(self.executor, self.decompress_data, chunk)
                await f.write(chunk)

        # ファイルのハッシュ値を検証
        if await self.calculate_file_hash(save_path) != self.file_hash:
            logger.error("File hash does not match.")
            sys.exit(1)

    def decompress_data(self, data):
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
    send_parser.add_argument('--version', type=str, default="3.0", help='Version of the nextdp protocol')

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
