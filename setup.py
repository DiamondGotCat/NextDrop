from setuptools import setup, find_packages

APP = ['NextDrop.py']
DATA_FILES = []
OPTIONS = {
    'argv_emulation': True,
    'includes': [
        'kamu-jp-modern',
        'PyQt6',
        'asyncio',
        'aiohttp',
        'aiohttp.web',
        'socket',
        'os',
        'sys',
        'gzip',
        'argparse',
        'concurrent.futures',
        'tqdm',
        'io',
        # 必要な他のモジュールをここに追加
    ],
    'excludes': ['packaging'],
    'packages': find_packages(),
    'plist': {
        'CFBundleName': 'NextDrop',
        'CFBundleDisplayName': 'NextDrop',
        'CFBundleGetInfoString': "NextDrop Application",
        'CFBundleIdentifier': "com.yourname.nextdrop",
        'CFBundleVersion': "0.1.0",
        'CFBundleShortVersionString': "0.1.0",
        'NSHighResolutionCapable': True,
    },
}

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
