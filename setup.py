from setuptools import setup

APP = ['gui.py'] 
DATA_FILES = []
OPTIONS = {
    'argv_emulation': True,
    'includes': ['PyQt6', 'asyncio', 'aiohttp', 'concurrent.futures'],  # 依存ライブラリを指定
    'packages': ['aiohttp', 'asyncio', 'concurrent', 'PyQt6']  # 使用するパッケージをリスト化
}

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
