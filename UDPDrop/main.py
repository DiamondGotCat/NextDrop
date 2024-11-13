# UDPDrop - High-Speed Data Pipeline Over UDP

import asyncio
import socket
import os
import sys
import zstandard as zstd
import argparse
from tqdm import tqdm
from io import BytesIO
import struct
from KamuJpModern.ModernLogging import ModernLogging

CHUNK_SIZE = 1024  # 1KB per UDP packet to avoid fragmentation
DEFAULT_MAX_CONCURRENT_TASKS = 100  # Maximum number of concurrent send tasks
COMPRESSION_LEVEL = 3  # zstd compression level (1-22)
TIMEOUT = 5  # Seconds to wait before considering a chunk lost
logger = ModernLogging("UDPDrop")

# Packet structure:
# For the first packet (chunk_number=0):
#   4 bytes: chunk_number (0)
#   2 bytes: filename length (unsigned short)
#   N bytes: filename (UTF-8)
#   4 bytes: total_chunks (unsigned int)
#   Remaining bytes: data
# For other packets:
#   4 bytes: chunk_number
#   Remaining bytes: data

class FileSender:
    def __init__(self, target, port, file_path, num_threads=4, compress=False, version="2.0"):
        self.target = target
        self.port = port
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress
        self.version = version
        self.zstd_compressor = zstd.ZstdCompressor(level=COMPRESSION_LEVEL) if self.compress else None
        self.loop = asyncio.get_event_loop()
        self.transport = None
        self.max_concurrent_tasks = DEFAULT_MAX_CONCURRENT_TASKS
        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        self.pending_chunks = {}
        self.ack_received = asyncio.Event()
        self.total_chunks = None
        self.filename = os.path.basename(self.file_path)

    async def send_file(self):
        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            total_chunks = 1
            logger.log("File size is 0. Setting total_chunks to 1.", "WARNING")
        else:
            total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        self.total_chunks = total_chunks

        if self.compress:
            logger.log("Sending in compression mode.", "INFO")

        # Create UDP socket
        transport, protocol = await self.loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(self),
            remote_addr=(self.target, self.port)
        )
        self.transport = transport

        tasks = []
        with open(self.file_path, 'rb') as f:
            progress_bar = tqdm(total=total_chunks, desc="Sending", unit="chunk")
            for chunk_number in range(total_chunks):
                chunk = f.read(CHUNK_SIZE)
                if self.compress:
                    chunk = self.zstd_compressor.compress(chunk)
                data = self.create_packet(chunk_number, chunk)
                await self.semaphore.acquire()
                task = asyncio.create_task(self.send_chunk(data, chunk_number, progress_bar))
                task.add_done_callback(lambda t: self.semaphore.release())
                tasks.append(task)
        await asyncio.gather(*tasks)
        progress_bar.close()
        self.transport.close()
        logger.log("File transfer completed.", "INFO")

    def create_packet(self, chunk_number, data):
        if chunk_number == 0:
            filename_bytes = self.filename.encode('utf-8')
            filename_length = len(filename_bytes)
            header = struct.pack('!I H', chunk_number, filename_length) + filename_bytes
            header += struct.pack('!I', self.total_chunks)
            packet = header + data
        else:
            header = struct.pack('!I', chunk_number)
            packet = header + data
        return packet

    async def send_chunk(self, data, chunk_number, progress_bar):
        try:
            self.transport.sendto(data)
            # For simplicity, we assume UDP is reliable. Implement ACKs if needed.
            progress_bar.update(1)
        except Exception as e:
            logger.log(f"Exception occurred while sending chunk {chunk_number}: {e}", "ERROR")

class UDPClientProtocol:
    def __init__(self, sender):
        self.sender = sender

    def connection_made(self, transport):
        pass  # Nothing to do

    def error_received(self, exc):
        logger.log(f"Error received: {exc}", "ERROR")

    def connection_lost(self, exc):
        logger.log("Socket closed, stopping sender.", "INFO")

class FileReceiver:
    def __init__(self, port, save_dir, compress=False):
        self.port = port
        self.save_dir = save_dir
        self.compress = compress
        self.chunks = {}
        self.filename = None
        self.total_chunks = None
        self.lock = asyncio.Lock()
        self.receive_bar = None
        self.zstd_decompressor = zstd.ZstdDecompressor() if self.compress else None

    async def start_server(self):
        loop = asyncio.get_event_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPServerProtocol(self),
            local_addr=('0.0.0.0', self.port)
        )
        logger.log(f"Receiver is listening on port {self.port}", "INFO")
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            transport.close()

    async def handle_packet(self, data, addr):
        if len(data) < 4:
            logger.log("Received packet too short to process.", "WARNING")
            return
        chunk_number = struct.unpack('!I', data[:4])[0]

        if chunk_number == 0:
            if len(data) < 10:
                logger.log("First chunk too short to contain metadata.", "WARNING")
                return
            filename_length = struct.unpack('!H', data[4:6])[0]
            if len(data) < 6 + filename_length + 4:
                logger.log("First chunk does not contain complete metadata.", "WARNING")
                return
            self.filename = data[6:6+filename_length].decode('utf-8')
            self.total_chunks = struct.unpack('!I', data[6+filename_length:10+filename_length])[0]
            self.receive_bar = tqdm(total=self.total_chunks, desc="Receiving", unit="chunk")
            logger.log(f"Receiving file '{self.filename}' with {self.total_chunks} chunks.", "INFO")
            chunk_data = data[10+filename_length:]
        else:
            chunk_data = data[4:]

        async with self.lock:
            if chunk_number not in self.chunks:
                self.chunks[chunk_number] = chunk_data
                if self.receive_bar:
                    self.receive_bar.update(1)

            if self.total_chunks is not None and len(self.chunks) == self.total_chunks:
                if self.receive_bar:
                    self.receive_bar.close()
                await self.save_file()

    async def save_file(self):
        if not self.chunks:
            logger.log("No chunks received. Aborting save.", "WARNING")
            return

        sorted_chunks = sorted(self.chunks.items())
        file_data = b''.join([chunk for _, chunk in sorted_chunks])

        if self.compress:
            logger.log("Decompressing data.", "INFO")
            try:
                file_data = self.zstd_decompressor.decompress(file_data)
            except Exception as e:
                logger.log(f"Failed to decompress: {e}", "ERROR")
                return

        save_path = os.path.join(self.save_dir, self.filename)

        try:
            with open(save_path, 'wb') as f:
                f.write(file_data)
            logger.log(f"File '{self.filename}' saved to '{self.save_dir}'.", "INFO")
            # Reset for next file
            self.chunks = {}
            self.filename = None
            self.total_chunks = None
            self.receive_bar = None
        except Exception as e:
            logger.log(f"Error while saving file: {e}", "ERROR")

class UDPServerProtocol:
    def __init__(self, receiver):
        self.receiver = receiver

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        asyncio.create_task(self.receiver.handle_packet(data, addr))

    def error_received(self, exc):
        logger.log(f"Server error received: {exc}", "ERROR")

    def connection_lost(self, exc):
        logger.log("Server connection closed.", "INFO")

def get_local_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

async def main():
    parser = argparse.ArgumentParser(description="High-Speed File Transfer Script Over UDP")
    subparsers = parser.add_subparsers(dest='mode', help='Specify the mode')

    # Send mode arguments
    send_parser = subparsers.add_parser('send', help='Send a file')
    send_parser.add_argument('target', type=str, help='Target IP address or local address (ending with .local)', default='127.0.0.1')
    send_parser.add_argument('--port', type=int, help='Target port number', default=4321)
    send_parser.add_argument('file_path', type=str, help='Path of the file to send')
    send_parser.add_argument('--threads', type=int, default=1, help='Number of threads to use (default: 1)')
    send_parser.add_argument('--compress', action='store_true', help='Compress the file before sending')
    send_parser.add_argument('--version', type=str, default="2.0", help='Version of the UDPDrop protocol (default: 2.0)')

    # Receive mode arguments
    receive_parser = subparsers.add_parser('receive', help='Receive a file')
    receive_parser.add_argument('--port', type=int, help='Port number to receive on', default=4321)
    receive_parser.add_argument('save_dir', type=str, help='Directory to save the received file')
    receive_parser.add_argument('--compress', action='store_true', help='Decompress the received file')

    args = parser.parse_args()

    if args.mode == 'send':
        if not os.path.isfile(args.file_path):
            logger.log(f"Error: File '{args.file_path}' does not exist.", "ERROR")
            sys.exit(1)
        sender = FileSender(args.target, args.port, args.file_path, args.threads, args.compress, args.version)
        await sender.send_file()
    elif args.mode == 'receive':
        if not os.path.isdir(args.save_dir):
            logger.log(f"Error: Directory '{args.save_dir}' does not exist.", "ERROR")
            sys.exit(1)
        receiver = FileReceiver(args.port, args.save_dir, args.compress)
        receiver_task = asyncio.create_task(receiver.start_server())
        try:
            await receiver_task
        except KeyboardInterrupt:
            logger.log("Keyboard interrupt detected. Exiting...", "INFO")
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log("Keyboard interrupt detected. Exiting...", "INFO")
