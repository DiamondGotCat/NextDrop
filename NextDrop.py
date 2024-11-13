# NextDrop - High-Speed Data Pipeline - GUI Updated Version

import sys
import os
import asyncio
import zstandard as zstd  # Changed from gzip to zstandard
import aiohttp
import aiohttp.web
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QFileDialog, QVBoxLayout, QHBoxLayout, QWidget,
    QPushButton, QLineEdit, QLabel, QCheckBox, QProgressBar, QListWidget
)
from PyQt6.QtCore import QThread, pyqtSignal, QObject, Qt
from PyQt6.QtGui import QIcon, QFont, QPalette, QColor, QIntValidator
from io import BytesIO
from pathlib import Path
import socket

CHUNK_SIZE = 1024 * 1024  # 1MB

# Fixed port number
FIXED_PORT = 4321

# Default save directory set to user's Downloads folder
DEFAULT_SAVE_DIR = str(Path.home() / "Downloads")

# ModernLogging class for logging
class ModernLogging:
    def __init__(self, name):
        self.name = name

    def log(self, message, level="INFO"):
        print(f"[{level}] {self.name}: {message}")

# FileSender class for sending files
class FileSender(QObject):
    progress_signal = pyqtSignal(int)
    queue_signal = pyqtSignal(str)

    def __init__(self, target, file_path, num_threads=4, compress=False, progress_callback=None, queue_callback=None, version="2.0"):
        super().__init__()
        self.target = target
        self.port = FIXED_PORT  # Use fixed port
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress
        self.logger = ModernLogging("FileSender")
        self.progress_callback = progress_callback
        self.queue_callback = queue_callback
        self.version = version
        self.zstd_compressor = zstd.ZstdCompressor(level=3) if self.compress else None
        self.semaphore = asyncio.Semaphore(100)  # Maximum number of concurrent tasks

    async def send_file(self):
        file_size = os.path.getsize(self.file_path)
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        if self.compress:
            self.logger.log("Sending in compression mode.", "INFO")
            if self.queue_callback:
                self.queue_callback.emit("Sending in compression mode...")

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=None)) as session:
            tasks = []
            with open(self.file_path, 'rb') as f:
                for i in range(total_chunks):
                    chunk = f.read(CHUNK_SIZE)
                    if self.compress:
                        compressed_chunk = self.zstd_compressor.compress(chunk)
                        data = compressed_chunk
                    else:
                        data = chunk
                    
                    await self.semaphore.acquire()
                    task = asyncio.create_task(self.send_chunk(session, data, i, total_chunks if i == 0 else None))
                    task.add_done_callback(lambda t: self.semaphore.release())
                    tasks.append(task)
                    
                    if self.progress_callback:
                        progress = int(((i + 1) / total_chunks) * 100)
                        self.progress_callback.emit(progress)
            await asyncio.gather(*tasks)
        
        if self.queue_callback:
            self.queue_callback.emit("Transfer completed!")

    async def send_chunk(self, session, chunk, chunk_number, total_chunks=None):
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
                    self.logger.log(f"Failed to send chunk {chunk_number}: Status {resp.status}", "ERROR")
                    if self.queue_callback:
                        self.queue_callback.emit(f"Failed to send chunk {chunk_number}.")
                else:
                    self.logger.log(f"Chunk {chunk_number} sent successfully.", "INFO")
        except Exception as e:
            self.logger.log(f"Exception occurred while sending chunk {chunk_number}: {e}", "ERROR")
            if self.queue_callback:
                self.queue_callback.emit(f"Error occurred while sending chunk {chunk_number}: {e}")

# FileReceiver class for receiving files
class FileReceiver(QObject):
    queue_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)

    def __init__(self, port, save_dir, compress=False, progress_callback=None):
        super().__init__()
        self.port = port
        self.save_dir = save_dir
        self.chunks = {}
        self.compress = compress
        self.lock = asyncio.Lock()
        self.filename = None
        self.total_chunks = None
        self.logger = ModernLogging("FileReceiver")
        self.progress_callback = progress_callback

    async def handle_upload(self, request):
        if request.path == '/upload' and request.method == 'POST':
            try:
                chunk_number = int(request.query.get('chunk_number', -1))
                if chunk_number == -1:
                    self.logger.log("Chunk number not specified.", "ERROR")
                    return aiohttp.web.Response(status=400, text="Chunk number not specified.")
                
                version = request.headers.get('nextdp-version', '1.0')

                data = await request.read()

                # Get filename and total chunks from the first chunk
                if chunk_number == 0:
                    self.filename = request.headers.get('X-Filename', f"received_file_{int(asyncio.get_event_loop().time())}")
                    self.total_chunks = int(request.headers.get('X-Total-Chunks', '1'))
                    self.logger.log(f"Received filename: {self.filename}", "INFO")
                    self.logger.log(f"Total chunks: {self.total_chunks}", "INFO")
                    self.logger.log(f"nextdp-version: {version}", "INFO")

                async with self.lock:
                    self.chunks[chunk_number] = data

                self.logger.log(f"Received chunk {chunk_number}. Size: {len(data)} bytes", "INFO")

                # Update progress
                if self.progress_callback and self.total_chunks:
                    progress = int((len(self.chunks) / self.total_chunks) * 100)
                    self.progress_callback.emit(progress)

                # Save file if all chunks are received
                if self.total_chunks is not None and len(self.chunks) == self.total_chunks:
                    self.logger.log("All chunks received. Saving file.", "INFO")
                    asyncio.create_task(self.save_file(version))

                return aiohttp.web.Response(status=200, text="Chunk received successfully.")
            except Exception as e:
                self.logger.log(f"Server error: {e}", "ERROR")
                return aiohttp.web.Response(status=500, text=f"Server error: {e}")
        self.logger.log(f"Unknown request path or method: {request.method} {request.path}", "WARNING")
        return aiohttp.web.Response(status=404, text="Not Found")

    async def start_server(self):
        app = aiohttp.web.Application(client_max_size=1024 * 1024 * 1024 * 20)  # Set maximum to 20GB
        app.router.add_post('/upload', self.handle_upload)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        self.logger.log(f"Receiver is listening on port {self.port}", "INFO")
        while True:
            await asyncio.sleep(3600)

    async def save_file(self, version):
        if not self.chunks:
            self.logger.log("No chunks received. Aborting save.", "WARNING")
            return
        
        self.queue_signal.emit(f"Saving file {self.filename}...")

        sorted_chunks = sorted(self.chunks.items())
        file_data = b''.join([chunk for _, chunk in sorted_chunks])

        if self.compress:
            self.logger.log("Decompressing data.", "INFO")
            try:
                if version == "2.0":
                    dctx = zstd.ZstdDecompressor()
                    file_data = dctx.decompress(file_data)
                else:
                    # Use gzip for version 1.0
                    import gzip
                    file_data = gzip.decompress(file_data)
            except Exception as e:
                self.logger.log("Failed to decompress. Please check the sender's compression settings.", "ERROR")
                return

        save_path = os.path.join(self.save_dir, self.filename)

        try:
            with open(save_path, 'wb') as f:
                f.write(file_data)
            self.logger.log(f"File '{self.filename}' saved to '{self.save_dir}'.", "INFO")
            self.queue_signal.emit(f"File {self.filename} has been saved!")
        except Exception as e:
            self.logger.log(f"Error while saving file: {e}", "ERROR")

# GUI integration using PyQt6
class SendWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    queue_signal = pyqtSignal(str)

    def __init__(self, target, file_path, num_threads, compress, version):
        super().__init__()
        self.target = target
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress
        self.version = version

    async def send_file_async(self):
        sender = FileSender(
            self.target,
            self.file_path,
            self.num_threads,
            self.compress,
            progress_callback=self.progress_signal,
            queue_callback=self.queue_signal,
            version=self.version
        )
        await sender.send_file()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.send_file_async())

class ReceiveWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    queue_signal = pyqtSignal(str)

    def __init__(self, port, save_dir, compress):
        super().__init__()
        self.port = port
        self.save_dir = save_dir
        self.compress = compress

    async def start_receiver_async(self):
        receiver = FileReceiver(
            self.port,
            self.save_dir,
            self.compress,
            progress_callback=self.progress_signal
        )
        receiver.queue_signal.connect(self.queue_signal)
        await receiver.start_server()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.start_receiver_async())

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("NextDrop - GUI")
        self.setGeometry(100, 100, 1000, 500)
        self.setWindowIcon(QIcon("icon.png"))  # Optional: Add a custom icon
        self.setStyleSheet("background-color: #2C3E50;")
        
        # Set a modern palette
        palette = QPalette()
        palette.setColor(QPalette.ColorRole.Window, QColor("#2C3E50"))
        palette.setColor(QPalette.ColorRole.WindowText, QColor("#ECF0F1"))
        palette.setColor(QPalette.ColorRole.Base, QColor("#34495E"))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor("#2C3E50"))
        palette.setColor(QPalette.ColorRole.ToolTipBase, QColor("#ECF0F1"))
        palette.setColor(QPalette.ColorRole.ToolTipText, QColor("#2C3E50"))
        palette.setColor(QPalette.ColorRole.Text, QColor("#ECF0F1"))
        palette.setColor(QPalette.ColorRole.Button, QColor("#3498DB"))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor("#ECF0F1"))
        palette.setColor(QPalette.ColorRole.BrightText, QColor("#E74C3C"))
        self.setPalette(palette)
        
        # Main layout as QVBoxLayout to add IP display at the top
        main_layout = QVBoxLayout()
        
        # IP Display Label
        ip_label = QLabel(f"{self.get_private_ip()}")
        ip_label.setStyleSheet("""
            color: #1ABC9C;
            font-size: 48px;
            font-weight: bold;
        """)
        ip_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(ip_label)
        
        # Horizontal layout for left and right sections
        horizontal_layout = QHBoxLayout()
        
        left_layout = QVBoxLayout()
        left_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        right_layout = QVBoxLayout()
        right_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        # Styling
        self.setStyleSheet("""
            QLabel {
                font-size: 14px;
                font-weight: bold;
                color: #ECF0F1;
            }
            QLineEdit, QTextEdit {
                font-size: 13px;
                padding: 5px;
                background-color: #34495E;
                color: #ECF0F1;
                border: 1px solid #1ABC9C;
                border-radius: 4px;
            }
            QPushButton {
                background-color: #1ABC9C;
                color: white;
                padding: 10px;
                border-radius: 5px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #16A085;
            }
            QCheckBox {
                font-size: 13px;
                color: #ECF0F1;
            }
        """)

        # File sending widgets (Top-left)
        send_label = QLabel("Send File")
        send_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        left_layout.addWidget(send_label)

        # IP Address Input Fields (4 x QLineEdit with dots)
        ip_input_layout = QHBoxLayout()
        self.ip_fields = []
        ip_validator = QIntValidator(0, 255, self)
        for i in range(4):
            ip_field = QLineEdit(self)
            ip_field.setMaxLength(3)
            ip_field.setValidator(ip_validator)
            ip_field.setFixedWidth(84)
            ip_field.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.ip_fields.append(ip_field)
            ip_input_layout.addWidget(ip_field)
            if i < 3:
                dot_label = QLabel(".")
                dot_label.setStyleSheet("color: #ECF0F1; font-size: 24px;")
                ip_input_layout.addWidget(dot_label)
        left_layout.addLayout(ip_input_layout)

        self.file_path_display = QLineEdit(self)
        self.file_path_display.setPlaceholderText("Path of the file to send")
        left_layout.addWidget(self.file_path_display)

        self.browse_button = QPushButton("Browse File", self)
        self.browse_button.clicked.connect(self.browse_file)
        left_layout.addWidget(self.browse_button)

        self.compress_checkbox = QCheckBox("Compress file before sending (Large files may become slower)", self)
        left_layout.addWidget(self.compress_checkbox)

        # nextdp version selection
        version_layout = QHBoxLayout()
        version_label = QLabel("nextdp Version:")
        self.version_input = QLineEdit(self)
        self.version_input.setPlaceholderText("e.g., 2.0")
        self.version_input.setFixedWidth(100)
        version_layout.addWidget(version_label)
        version_layout.addWidget(self.version_input)
        left_layout.addLayout(version_layout)

        self.send_button = QPushButton("Send", self)
        self.send_button.clicked.connect(self.send_file)
        left_layout.addWidget(self.send_button)

        # Progress bar and queue list for sending and receiving (Right-side)
        send_progress_label = QLabel("Sending Progress:")
        send_progress_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        right_layout.addWidget(send_progress_label)

        # Progress bar for sending
        self.send_progress_bar = QProgressBar(self)
        self.send_progress_bar.setValue(0)
        right_layout.addWidget(self.send_progress_bar)

        receive_progress_label = QLabel("Receiving Progress:")
        receive_progress_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        right_layout.addWidget(receive_progress_label)
      
        # Progress bar for receiving
        self.receive_progress_bar = QProgressBar(self)
        self.receive_progress_bar.setValue(0)
        right_layout.addWidget(self.receive_progress_bar)

        # Queue list for sending
        self.send_queue_list = QListWidget(self)
        right_layout.addWidget(self.send_queue_list)

        # Adding left and right layouts to the horizontal layout
        horizontal_layout.addLayout(left_layout)
        horizontal_layout.addLayout(right_layout)

        # Adding the horizontal layout to the main vertical layout
        main_layout.addLayout(horizontal_layout)

        # Main widget settings
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        self.dummy_network_request()

        # Start receiving on application launch
        self.start_receiving()

    def get_private_ip(self):
        """Retrieve the local machine's private IPv4 address"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Dummy connection to get IP address
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        except Exception:
            IP = '127.0.0.1'
        finally:
            s.close()
        return IP
    
    async def dummy_network_request(self):
        try:
            async with aiohttp.ClientSession() as session:
                # Attempt to connect to own server
                async with session.get(f'http://{self.get_private_ip()}:{FIXED_PORT}') as resp:
                    pass  # Dummy request to trigger permission prompt
        except Exception as e:
            # No problem if server is not yet started
            self.log(f"Dummy network request failed: {e}")

    def log(self, message):
        self.send_queue_list.addItem(message)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select File")
        if file_path:
            self.file_path_display.setText(file_path)

    def send_file(self):
        # Get IP address from the four fields and concatenate
        ip_parts = []
        for field in self.ip_fields:
            text = field.text()
            if not text:
                self.send_queue_list.addItem("Error: IP address field is empty")
                return
            ip_parts.append(text)
        target = ".".join(ip_parts)

        file_path = self.file_path_display.text()
        compress = self.compress_checkbox.isChecked()
        num_threads = 4  # Set number of threads as needed

        version = self.version_input.text().strip() or "2.0"  # Version input

        if not os.path.isfile(file_path):
            self.send_queue_list.addItem("Error: File does not exist")
            return

        if not target:
            self.send_queue_list.addItem("Error: Target IP address is required")
            return

        # Validate each part of the IP address
        try:
            parts = target.split('.')
            if len(parts) != 4:
                raise ValueError("Invalid IP address format")
            for part in parts:
                num = int(part)
                if num < 0 or num > 255:
                    raise ValueError("Each part of the IP address must be between 0 and 255")
        except ValueError as ve:
            self.send_queue_list.addItem(f"Error: {ve}")
            return

        # Validate version number
        if not self.validate_version(version):
            self.send_queue_list.addItem("Error: Version number must consist of numbers and dots only (e.g., 2.0)")
            return

        # Start SendWorker thread
        self.send_worker = SendWorker(target, file_path, num_threads, compress, version)
        self.send_worker.log_signal.connect(self.log)
        self.send_worker.progress_signal.connect(self.send_progress_bar.setValue)
        self.send_worker.queue_signal.connect(lambda msg: self.send_queue_list.addItem(msg))
        self.send_worker.start()

    def validate_version(self, version):
        """Validate the format of the version number"""
        import re
        pattern = r'^\d+\.\d+$'
        return re.match(pattern, version) is not None

    # Automate receiving functionality
    def start_receiving(self):
        port = FIXED_PORT  # Fixed port
        save_dir = DEFAULT_SAVE_DIR  # Fixed save directory
        compress = False  # Set as needed

        # Start ReceiveWorker thread
        self.receive_worker = ReceiveWorker(port, save_dir, compress)
        self.receive_worker.log_signal.connect(self.log)
        self.receive_worker.progress_signal.connect(self.receive_progress_bar.setValue)
        self.receive_worker.queue_signal.connect(lambda msg: self.send_queue_list.addItem(msg))
        self.receive_worker.start()

# Main application execution
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
