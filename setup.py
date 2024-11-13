from setuptools import setup, find_packages

setup(
    name="next-drop-lib",
    version="2.0.1",
    description="A high-speed data pipeline library",
    author="DiamondGotCat",
    author_email="chii@kamu.jp",
    packages=find_packages(),
    install_requires=[
        "aiohttp",
        "tqdm",
        "zstandard",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
