#!/bin/bash

set -e

# Jump to the current directory first
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../" >/dev/null 2>&1 && pwd )"
cd "${DIR}" || exit

if [[ ! -f .base.env ]]; then
    >&2 echo ".base.env file not found"
    exit 1
fi

# https://gist.github.com/mihow/9c7f559807069a03e302605691f85572#gistcomment-2706921
# shellcheck disable=SC2046
export $(sed 's/#.*//g' .base.env | xargs)

exec pipenv run blockchain-etl dump2 \
    --chain=ethereum \
    --lag=0 \
    --pending-mode \
    --max-workers=10 \
    --block-batch-size=10 \
    --batch-size=20 \
    --start-block=-1 \
    --provider-uri="${BLOCKCHAIN_ETL_PROVIDER_URI}" \
    --enable-enrich \
    --cache-path="${BLOCKCHAIN_ETL_CACHE_PATH}/eth-dump-token-cache" \
    --entity-types="block,transaction,trace,log,token_transfer" \
    --last-synced-block-file=.priv/ethereum/pending-tsdb-lsf.txt \
    --target-db-url="$BLOCKCHAIN_ETL_DSDB_URL" \
    --provider-is-geth=true
