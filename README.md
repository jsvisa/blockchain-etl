<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [The Blockchain ETL](#the-blockchain-etl)
  - [design docs](#design-docs)
  - [Preparation](#preparation)
    - [macOS](#macos)
    - [Linux](#linux)
  - [Install](#install)
  - [Run as as Python package](#run-as-as-python-package)
  - [Run {btc-eth}-etl](#run-btc-eth-etl)
    - [run bitcoin dump && load](#run-bitcoin-dump--load)
    - [run ethereum dump && load](#run-ethereum-dump--load)
  - [Production Run](#production-run)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# The Blockchain ETL

## design docs

- [The Design of AlertEngine](./docs/alert-engine.md)
- [The Design of TopHolder](./docs/top-holder.md)

**ONLY Python 3.9+ is supported**

**Chain version required**

| Chain   | Required | Note                                                                       |
| ------- | -------- | -------------------------------------------------------------------------- |
| Bitcoin | `<22.0`  | see [docs/bitcoin-22.0-rpc.md](./docs/bitcoin-22.0-rpc.md) for more detail |

## Preparation

Before running this project, you need to install the following packges on your system.

```bash
gcc
python3-dev
libpq or libpq-dev or postgresql-devel
pipenv
```

### macOS

```bash
brew install libpq python3-dev
pip3 install pipenv
```

### Linux

```bash
sudo apt install gcc libpq-dev python3-dev
sudo yum install gcc postgresql-devel python3-devel
pip3 install pipenv
```

## Install

```bash
make setup
pipenv shell
```

## Run as as Python package

> install into system

```bash
pip install git+https://github.com/jsvisa/blockchain-etl.git@v${version}
```

> or used in Pipfile

Add this line into your Pipfile and then run `pipenv install`

```
blockchain-etl = {git = "https://github.com/jsvisa/blockchain-etl.git@v${version}"}
```

## Run {btc-eth}-etl

The [etl](./etl) is the entrypoint to everything, all you need to do is `./etl -h` to see the help messages.

```bash
./etl -h
./etl -c bitcion -h
./etl -c ethereum -h
```

The most important tools in ETL are [dump](./blockchainetl/cli/dump.py) and [load](./blockchainetl/cli/load.py). The former takes the rawdata out and parsed through JSONRPC, then writes it to a local CSV file while writing a log to Redis STREAM; the later subscribes the Redis STREAM, and COPY the CSV file to GreenPlum.

### run bitcoin dump && load

> dump

```bash
mkdir -p logs
./etl dump2 \
    --chain=bitcoin \
    --lag=20 \
    --max-workers=10 \
    --block-batch-size=10 \
    --batch-size=100 \
    --start-block=1024 \
    --provider-uri="$BLOCKCHAIN_ETL_PROVIDER_URI" \
    --target-db-url="$BLOCKCHAIN_ETL_TSDB_URL" \
    --enable-enrich \
    --entity-types="block,transaction,trace" \
    --last-synced-block-file=.priv/bitcoin/stream-tsdb-lsf.txt \
    >> ./logs/btc.dump.log 2>&1
```

### run ethereum dump && load

Similar to bitcoin's dump

> dump

```bash
mkdir -p logs
./etl dump2 \
    --chain=ethereum \
    --lag=80 \
    --max-workers=10 \
    --block-batch-size=10 \
    --batch-size=50 \
    --provider-uri="$BLOCKCHAIN_ETL_PROVIDER_URI" \
    --target-db-url="$BLOCKCHAIN_ETL_TSDB_URL" \
    --enable-enrich \
    --cache-path="${BLOCKCHAIN_ETL_CACHE_PATH}/eth-dump-token-cache" \
    --entity-types="block,transaction,receipt,log,token_transfer,trace,contract" \
    --last-synced-block-file=.priv/ethereum/stream-tsdb-lsf.txt \
    >> ./logs/btc.dump.log 2>&1
```

We assume that the provider is [Parity Ethereum Client - OpenEthereum | Parity Technologies](https://www.parity.io/technologies/ethereum/), if the provider is [Go Ethereum](https://geth.ethereum.org/), you need to append a parameter `--provider-is-geth=true`

## Production Run

We recommend using supervisord to manage all processes, and also recommend using Prometheus to monitor the progress of each dump process.

See [etl-runner](./etl-runner) for more detail
