from setuptools import setup

APP = ['NextDrop.py'] 
DATA_FILES = []
OPTIONS = {
    'argv_emulation': True,
    'includes': ['PyQt6', 'asyncio', 'aiohttp', 'concurrent.futures'],
    'excludes': ['packaging'],  # 衝突するライブラリを除外
    'packages': ['aiohttp', 'asyncio', 'concurrent', 'PyQt6'],
}

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
