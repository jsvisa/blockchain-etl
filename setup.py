import os
from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


# copy from https://github.com/EthTx/ethtx/blob/master/setup.py
def load_requirements(fname):
    import toml

    """Load requirements from file."""
    try:
        with open(fname, "r") as fh:
            pipfile = fh.read()
        pipfile_toml = toml.loads(pipfile)
    except FileNotFoundError:
        return []

    try:
        required_packages = pipfile_toml["packages"].items()
    except KeyError:
        return []

    packages = []
    for pkg, ver in required_packages:
        package = pkg
        if isinstance(ver, str) and ver != "*":
            package += ver
        elif isinstance(ver, dict) and len(ver) == 1:
            k, v = list(ver.items())[0]
            package += f" @ {k}+{v}#egg={pkg}"
        packages.append(package)
    return packages


long_description = read("README.md") if os.path.isfile("README.md") else ""

setup(
    name="blockchain-etl",
    version="3.0.3",
    author="Delweng Zheng",
    author_email="delweng@gmail.com",
    description="Tools for exporting Ethereum/Bitcoin blockchain data into CSV/PostgreSQL",
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6,<4",
    install_requires=[
        "click==8.0.3",
        "pymysql",
        "requests",
        "eth-hash",
        "pandas==1.4.0",
        "web3>5.0.0,<6.0",
        "pyyaml",
        "sqlalchemy<2.0",
        "jinja2",
        "psycopg2-binary",
        "eth-abi",
        "redis",
        "ethereum-dasm",
        "base58",
        "pyetherchain",
        "ecdsa",
        "chainside-btcpy",
        "ply",
        "timeout-decorator",
        "google.cloud",
        "pyyaml-include",
        "cachetools",
        "s3fs==2022.1.0",
        "millify==0.1.1",
        "diskcache==5.4.0",
        "loky==3.1.0",
        "simplejson==3.17.6",
        "jsonlines",
        "prometheus_client",
        "rpq==2.2",
        "pypeln",
        "kafka-python",
        "multicall @ git+https://github.com/jsvisa/multicall.py.git#egg=multicall",
    ],
    entry_points={
        "console_scripts": [
            "blockchain-etl=blockchainetl.cli:cli",
        ],
    },
)
