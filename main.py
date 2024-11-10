# NextDrop - High-Speed Data Pipeline

from KamuJpModern.ModernLogging import ModernLogging
from KamuJpModern.ModernProgressBar import ModernProgressBar
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
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        if self.compress:
            logger.log("Sending in compression mode.", "INFO")

        async with aiohttp.ClientSession() as session:
            tasks = []
            with open(self.file_path, 'rb') as f:
                progress_bar = ModernProgressBar(total_chunks, "Compressing", 32)
                progress_bar.start()
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
                    progress_bar.update(1)
                    task = asyncio.create_task(self.send_chunk(session, data, i, total_chunks if i == 0 else None))
                    tasks.append(task)
            await asyncio.gather(*tasks)

    async def send_chunk(self, session, chunk, chunk_number, total_chunks=None):
        url = f'http://{self.target}:{self.port}/upload?chunk_number={chunk_number}'
        headers = {}
        if chunk_number == 0:
            headers['X-Filename'] = os.path.basename(self.file_path)
            headers['X-Total-Chunks'] = str(total_chunks)
        try:
            async with session.post(url, data=chunk, headers=headers) as resp:
                if resp.status != 200:
                    logger.log(f"チャンク {chunk_number} の送信に失敗しました: ステータス {resp.status}", "ERROR")
        except Exception as e:
            logger.log(f"チャンク {chunk_number} の送信中に例外が発生しました: {e}", "ERROR")

class FileReceiver:
    def __init__(self, port, save_dir, compress=False):
        self.port = port
        self.save_dir = save_dir
        self.chunks = {}
        self.compress = compress
        self.lock = asyncio.Lock()
        self.filename = None
        self.total_chunks = None

    async def handle_upload(self, request):
        if request.path == '/upload' and request.method == 'POST':
            try:
                chunk_number = int(request.query.get('chunk_number', -1))
                if chunk_number == -1:
                    logger.log("チャンク番号が指定されていません", "ERROR")
                    return aiohttp.web.Response(status=400, text="チャンク番号が指定されていません")
                data = await request.read()

                # 最初のチャンクでファイル名と総チャンク数を取得
                if chunk_number == 0:
                    self.filename = request.headers.get('X-Filename', f"received_file_{int(asyncio.get_event_loop().time())}")
                    self.total_chunks = int(request.headers.get('X-Total-Chunks', '1'))
                    logger.log(f"受信ファイル名: {self.filename}", "INFO")
                    logger.log(f"総チャンク数: {self.total_chunks}", "INFO")

                async with self.lock:
                    self.chunks[chunk_number] = data

                logger.log(f"チャンク {chunk_number} を受信しました。サイズ: {len(data)} バイト", "INFO")

                # すべてのチャンクを受信した場合、ファイルを保存
                if self.total_chunks is not None and len(self.chunks) == self.total_chunks:
                    logger.log("すべてのチャンクを受信しました。ファイルを保存します。", "INFO")
                    asyncio.create_task(self.save_file())

                return aiohttp.web.Response(status=200, text="チャンクを受信しました")
            except Exception as e:
                logger.log(f"サーバーエラー: {e}", "ERROR")
                return aiohttp.web.Response(status=500, text=f"サーバーエラー: {e}")
        logger.log(f"不明なリクエストパスまたはメソッド: {request.method} {request.path}", "WARNING")
        return aiohttp.web.Response(status=404, text="見つかりません")

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
            logger.log("受信したチャンクがありません。保存を中止します。", "WARNING")
            return

        sorted_chunks = sorted(self.chunks.items())
        file_data = b''.join([chunk for _, chunk in sorted_chunks])

        if self.compress:
            logger.log("データを解凍中です。", "INFO")
            try:
                file_data = gzip.decompress(file_data)
            except gzip.BadGzipFile:
                logger.log("解凍に失敗しました。送信側の圧縮設定を確認してください。", "ERROR")
                return

        save_path = os.path.join(self.save_dir, self.filename)

        try:
            with open(save_path, 'wb') as f:
                f.write(file_data)
            logger.log(f"ファイル '{self.filename}' を '{self.save_dir}' に保存しました。", "INFO")
        except Exception as e:
            logger.log(f"ファイルの保存中にエラーが発生しました: {e}", "ERROR")

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
        try:
            await receiver_task
        except KeyboardInterrupt:
            logger.log("Receiving stopped.", "INFO")
            sys.exit(0)
    
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
