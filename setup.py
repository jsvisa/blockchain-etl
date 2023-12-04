import os
from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read("README.md") if os.path.isfile("README.md") else ""

setup(
    name="blockchain-etl",
    version="3.4.2",
    author="Delweng Zheng",
    author_email="delweng@gmail.com",
    description="Tools for exporting Ethereum/Bitcoin data into CSV/PostgreSQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jsvisa/blockchain-etl",
    packages=find_packages(
        exclude=[
            "bin",
            "logs",
            "testdata",
            "tests",
            "etl-runner",
        ]
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.6,<4",
    install_requires=[
        "click==8.0.3",
        "pymysql",
        "requests",
        "eth-hash",
        "pandas==1.4.0",
        "web3>=6",
        "pyyaml",
        "sqlalchemy<2.0",
        "jinja2",
        "psycopg2-binary",
        "eth-abi",
        "redis",
        "ethereum-dasm",
        "base58",
        "ecdsa",
        "chainside-btcpy",
        "ply",
        "pyyaml-include",
        "cachetools",
        "millify==0.1.1",
        "diskcache==5.4.0",
        "loky>=3.1.0",
        "jsonlines",
        "prometheus_client",
        "rpq==2.2",
        "pypeln",
    ],
    entry_points={
        "console_scripts": [
            "blockchain-etl=blockchainetl.cli:cli",
        ],
    },
)
