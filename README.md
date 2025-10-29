# NextDrop
NextDrop is a high-speed data pipeline capable of transferring large files efficiently, with a peak transfer rate allowing for 3GB of data to be received in under 10 seconds, making it a standout system for rapid file transfers.

**Speed in Peak transfer rate (Latest):** 3GB /5sec

## DiamondGotCat's Protocols

### UltraDP: Most Fast File Transfer Protocol in My Projects
8 Gbps(1 GB/s) in My Wi-fi

### NextDP(NextDrop): You can use Official Python Library
4.8 Gbps(0.6 GB/s) in My Wi-fi

### USFTP: Built-in File integrity check function (SHA-256)
2 Gbps(0.25 GB/s) in My Wi-fi

## Installation

To install NextDrop, ensure you have python33.7+ installed along with the required dependencies. Run the following command:

**Library Version**
```bash
pip3 install next-drop-lib
```

**Command-Line Version Requiments (`main.py`)**

```bash
pip3 install nercone-modern aiohttp tqdm zstandard
```

**GUI Version Requiments (`NextDrop.py`)**

```bash
pip3 install nercone-modern PyQt6 aiohttp tqdm zstandard
```

## Usage

NextDrop operates in two modes: sending and receiving. Below are the details for each mode.

### Library (Example)
```python
import asyncio
from next_drop_lib import FileSender, FileReceiver, NextDropGeneral
import threading
import time

async def file_send_example():
    await FileSender("localhost", port=4321, file_path="./test.txt").send_file()

async def file_receive_example():
    await FileReceiver(port=4321, save_dir="./recived/").start_server()
    await asyncio.sleep(5)

def start_server():
    asyncio.run(file_receive_example())

if __name__ == '__main__':
    # Start the server in a separate thread
    threading.Thread(target=start_server).start()

    # Start the file sender
    asyncio.run(file_send_example())
```

### CLI Sending Mode

To send a file, use the following command:

```bash
python3nextdrop.py send <target_ip> --port <port> <file_path> [--threads <num_threads>]
```

- `<target_ip>`: IP address of the receiving server.
- `--port <port>`: Port on which the receiver is listening (default is `4321`).
- `<file_path>`: Path to the file you want to send.
- `--threads <num_threads>`: Number of threads to use (default is `1`).

#### Example

```bash
python3nextdrop.py send 192.168.1.10 --port 4321 /path/to/file.zip --threads 4
```

### CLI Receiving Mode

To start a receiver server, use the following command:

```bash
python3 nextdrop.py receive --port <port> <save_dir> [--compress]
```

- `--port <port>`: Port to listen on for incoming file transfers (default is `4321`).
- `<save_dir>`: Directory where received files will be saved.

#### Example

```bash
python3 nextdrop.py receive --port 4321 /path/to/save --compress
```

