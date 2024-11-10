# NextDrop - High-Speed Data Pipeline - GUI

import sys
import os
import asyncio
import gzip
import aiohttp
import aiohttp.web
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QFileDialog, QVBoxLayout, QHBoxLayout, QWidget,
    QPushButton, QLineEdit, QLabel, QCheckBox, QSpinBox, QTextEdit, QFrame, QProgressBar, QListWidget
)
from PyQt6.QtCore import QThread, pyqtSignal, QObject, Qt
from PyQt6.QtGui import QIcon, QFont, QPalette, QColor
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

CHUNK_SIZE = 1024 * 1024  # 1MB

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

    def __init__(self, target, port, file_path, num_threads=4, compress=False, progress_callback=None, queue_callback=None):
        super().__init__()
        self.target = target
        self.port = port
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress
        self.logger = ModernLogging("FileSender")
        self.progress_callback = progress_callback
        self.queue_callback = queue_callback

    async def send_file(self):
        file_size = os.path.getsize(self.file_path)
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        if self.compress:
            self.logger.log("Sending in compression mode.", "INFO")
            if self.queue_callback:
                self.queue_callback.emit("圧縮モードで送信中...")

        async with aiohttp.ClientSession() as session:
            tasks = []
            with open(self.file_path, 'rb') as f:
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
                    
                    task = asyncio.create_task(self.send_chunk(session, data, i, total_chunks if i == 0 else None))
                    tasks.append(task)
                    
                    if self.progress_callback:
                        progress = int(((i + 1) / total_chunks) * 100)
                        self.progress_callback.emit(progress)
            await asyncio.gather(*tasks)
        
        if self.queue_callback:
            self.queue_callback.emit("Sent!")

    async def send_chunk(self, session, chunk, chunk_number, total_chunks=None):
        url = f'http://{self.target}:{self.port}/upload?chunk_number={chunk_number}'
        headers = {}
        if chunk_number == 0:
            headers['X-Filename'] = os.path.basename(self.file_path)
            headers['X-Total-Chunks'] = str(total_chunks)
        try:
            async with session.post(url, data=chunk, headers=headers) as resp:
                if resp.status != 200:
                    self.logger.log(f"Failed to send chunk {chunk_number}: Status {resp.status}", "ERROR")
                else:
                    self.logger.log(f"Successfully sent chunk {chunk_number}.", "INFO")
        except Exception as e:
            self.logger.log(f"Exception occurred while sending chunk {chunk_number}: {e}", "ERROR")
            if self.queue_callback:
                self.queue_callback.emit(f"チャンク {chunk_number} の送信中にエラーが発生しました: {e}")

# FileReceiver class for receiving files
class FileReceiver(QObject):
    queue_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    queue_signal = pyqtSignal(str)

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
                    self.logger.log("チャンク番号が指定されていません", "ERROR")
                    return aiohttp.web.Response(status=400, text="チャンク番号が指定されていません")
                data = await request.read()

                # 最初のチャンクでファイル名と総チャンク数を取得
                if chunk_number == 0:
                    self.filename = request.headers.get('X-Filename', f"received_file_{int(asyncio.get_event_loop().time())}")
                    self.total_chunks = int(request.headers.get('X-Total-Chunks', '1'))
                    self.logger.log(f"受信ファイル名: {self.filename}", "INFO")
                    self.logger.log(f"総チャンク数: {self.total_chunks}", "INFO")

                async with self.lock:
                    self.chunks[chunk_number] = data

                self.logger.log(f"Received chunk {chunk_number}. Size: {len(data)} bytes", "INFO")

                # Update progress
                if self.progress_callback and self.total_chunks:
                    self.progress_callback.emit(100)

                # すべてのチャンクを受信した場合、ファイルを保存
                if self.total_chunks is not None and len(self.chunks) == self.total_chunks:
                    self.logger.log("All chunks received. Saving file.", "INFO")
                    asyncio.create_task(self.save_file())

                return aiohttp.web.Response(status=200, text="Chunk received")
            except Exception as e:
                self.logger.log(f"Server error: {e}", "ERROR")
                return aiohttp.web.Response(status=500, text=f"Server error: {e}")
        self.logger.log(f"Unknown request path or method: {request.method} {request.path}", "WARNING")
        return aiohttp.web.Response(status=404, text="Not found")

    async def start_server(self):
        app = aiohttp.web.Application(client_max_size=1024 * 1024 * 1024 * 5)  # 最大5GBに設定
        app.router.add_post('/upload', self.handle_upload)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        self.logger.log(f"Receiver is listening on port {self.port}", "INFO")
        while True:
            await asyncio.sleep(3600)

    async def save_file(self):
        if not self.chunks:
            self.logger.log("No received chunks. Saving aborted.", "WARNING")
            return
        
        self.queue_signal.emit(f"Received {self.filename}, saving...")

        sorted_chunks = sorted(self.chunks.items())
        file_data = b''.join([chunk for _, chunk in sorted_chunks])

        if self.compress:
            self.logger.log("Decompressing data.", "INFO")
            try:
                file_data = gzip.decompress(file_data)
            except gzip.BadGzipFile:
                self.logger.log("Decompression failed. Check the compression setting on the sender side.", "ERROR")
                return

        save_path = os.path.join(self.save_dir, self.filename)

        try:
            with open(save_path, 'wb') as f:
                f.write(file_data)
            self.logger.log(f"File '{self.filename}' saved to '{self.save_dir}'.", "INFO")
            self.queue_signal.emit(f"Saved {self.filename}!")
        except Exception as e:
            self.logger.log(f"An error occurred while saving the file: {e}", "ERROR")

# GUI integration using PyQt6
class SendWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    queue_signal = pyqtSignal(str)

    def __init__(self, target, port, file_path, num_threads, compress):
        super().__init__()
        self.target = target
        self.port = port
        self.file_path = file_path
        self.num_threads = num_threads
        self.compress = compress

    async def send_file(self):
        sender = FileSender(self.target, self.port, self.file_path, self.num_threads, self.compress, 
                            progress_callback=self.progress_signal, queue_callback=self.queue_signal)
        await sender.send_file()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.send_file())

class ReceiveWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    queue_signal = pyqtSignal(str)

    def __init__(self, port, save_dir, compress):
        super().__init__()
        self.port = port
        self.save_dir = save_dir
        self.compress = compress

    async def start_receiver(self):
        receiver = FileReceiver(self.port, self.save_dir, self.compress, progress_callback=self.progress_signal)
        receiver.progress_signal.connect(self.progress_signal)
        receiver.queue_signal.connect(self.queue_signal)
        await receiver.start_server()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.start_receiver())

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("NextDrop - GUI")
        self.setGeometry(100, 100, 1000, 600)
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
        
        # Main layout
        main_layout = QHBoxLayout()
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
            QLineEdit, QSpinBox, QTextEdit {
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
        send_label = QLabel("File sending")
        send_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        left_layout.addWidget(send_label)

        self.target_input = QLineEdit(self)
        self.target_input.setPlaceholderText("Target IP address")
        left_layout.addWidget(self.target_input)

        self.port_input = QSpinBox(self)
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(4321)
        left_layout.addWidget(self.port_input)

        self.file_path_display = QLineEdit(self)
        self.file_path_display.setPlaceholderText("Path to the file you want to send")
        left_layout.addWidget(self.file_path_display)

        self.browse_button = QPushButton("Select file", self)
        self.browse_button.clicked.connect(self.browse_file)
        left_layout.addWidget(self.browse_button)

        self.compress_checkbox = QCheckBox("Send file in compression (slow for large files)", self)
        left_layout.addWidget(self.compress_checkbox)

        self.send_button = QPushButton("Send", self)
        self.send_button.clicked.connect(self.send_file)
        left_layout.addWidget(self.send_button)

        # File receiving widgets (Bottom-left)
        receive_label = QLabel("File receiving")
        receive_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        left_layout.addWidget(receive_label)

        self.receive_port_input = QSpinBox(self)
        self.receive_port_input.setRange(1, 65535)
        self.receive_port_input.setValue(4321)
        left_layout.addWidget(self.receive_port_input)

        self.save_dir_display = QLineEdit(self)
        self.save_dir_display.setPlaceholderText("Save directory")
        left_layout.addWidget(self.save_dir_display)

        self.browse_dir_button = QPushButton("Select directory", self)
        self.browse_dir_button.clicked.connect(self.browse_dir)
        left_layout.addWidget(self.browse_dir_button)

        self.decompress_checkbox = QCheckBox("Decompress received file (if compressed)", self)
        left_layout.addWidget(self.decompress_checkbox)

        self.receive_button = QPushButton("Start receiving", self)
        self.receive_button.clicked.connect(self.receive_file)
        left_layout.addWidget(self.receive_button)

        # Progress bar and queue list for sending and receiving (Right-side)
        progress_label = QLabel("Sending:")
        progress_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        right_layout.addWidget(progress_label)

        # Progress bar for sending
        self.send_progress_bar = QProgressBar(self)
        self.send_progress_bar.setValue(0)
        right_layout.addWidget(self.send_progress_bar)

        # Progress bar and queue list for sending and receiving (Right-side)
        progress_label = QLabel("Receiving:")
        progress_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        right_layout.addWidget(progress_label)
      
        # Progress bar for receiving
        self.receive_progress_bar = QProgressBar(self)
        self.receive_progress_bar.setValue(0)
        right_layout.addWidget(self.receive_progress_bar)

        # Queue list for sending
        self.send_queue_list = QListWidget(self)
        right_layout.addWidget(self.send_queue_list)
        
        # Adding layouts to the main layout
        main_layout.addLayout(left_layout)
        main_layout.addLayout(right_layout)

        # Main widget settings
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

    def log(self, message):
        self.log_display.append(message)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select file")
        if file_path:
            self.file_path_display.setText(file_path)

    def browse_dir(self):
        directory = QFileDialog.getExistingDirectory(self, "Select directory")
        if directory:
            self.save_dir_display.setText(directory)

    def send_file(self):
        target = self.target_input.text()
        port = self.port_input.value()
        file_path = self.file_path_display.text()
        compress = self.compress_checkbox.isChecked()
        num_threads = 4  # Set the number of threads as needed

        if not os.path.isfile(file_path):
            self.log("Error: File does not exist")
            return

        # Start the send worker thread
        self.send_worker = SendWorker(target, port, file_path, num_threads, compress)
        self.send_worker.log_signal.connect(self.log)
        self.send_worker.progress_signal.connect(self.send_progress_bar.setValue)
        self.send_worker.queue_signal.connect(self.send_queue_list.addItem)
        self.send_worker.start()

    def receive_file(self):
        port = self.receive_port_input.value()
        save_dir = self.save_dir_display.text()
        compress = self.decompress_checkbox.isChecked()

        if not os.path.isdir(save_dir):
            self.log("Error: Directory does not exist")
            return

        # Start the receive worker thread
        self.receive_worker = ReceiveWorker(port, save_dir, compress)
        self.receive_worker.log_signal.connect(self.log)
        self.receive_worker.progress_signal.connect(self.receive_progress_bar.setValue)
        self.receive_worker.start()

app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec())