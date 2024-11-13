# NextDrop
NextDrop is a high-speed data pipeline capable of transferring large files efficiently, with a peak transfer rate allowing for 3GB of data to be received in under 10 seconds, making it a standout system for rapid file transfers.

**Speed in Peak transfer rate:** 3GB /5sec

## Installation

To install NextDrop, ensure you have Python 3.7+ installed along with the required dependencies. Run the following command:

**Command-Line Version (`main.py`)**

```bash
pip install kamu-jp-modern aiohttp tqdm zstandard
```

**GUI Version (`NextDrop.py`)**

```bash
pip install kamu-jp-modern PyQt6 aiohttp tqdm zstandard
```

## Usage

NextDrop operates in two modes: sending and receiving. Below are the details for each mode.

### Sending Mode

To send a file, use the following command:

```bash
python nextdrop.py send <target_ip> --port <port> <file_path> [--threads <num_threads>]
```

- `<target_ip>`: IP address of the receiving server.
- `--port <port>`: Port on which the receiver is listening (default is `4321`).
- `<file_path>`: Path to the file you want to send.
- `--threads <num_threads>`: Number of threads to use (default is `1`).

#### Example

```bash
python nextdrop.py send 192.168.1.10 --port 4321 /path/to/file.zip --threads 4
```

### Receiving Mode

To start a receiver server, use the following command:

```bash
python nextdrop.py receive --port <port> <save_dir> [--compress]
```

- `--port <port>`: Port to listen on for incoming file transfers (default is `4321`).
- `<save_dir>`: Directory where received files will be saved.

#### Example

```bash
python nextdrop.py receive --port 4321 /path/to/save --compress
```

