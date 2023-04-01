import os

IGNORE_TRACE_ERROR = os.getenv("BLOCKCHAIN_ETL_IGNORE_TRACE_ERROR") == "1"

# the default debug_xxx api's timeout is 5s for each tx,
# in some cases(with old CPU) it needs more time to complete.
GETH_DEBUG_API_TIMEOUT = os.getenv("BLOCKCHAIN_ETL_GETH_DEBUG_API_TIMEOUT", "60s")
GETH_TRACE_MODULE = os.getenv("BLOCKCHAIN_ETL_GETH_TRACE_MODULE", "callTracer")
GETH_TRACE_BY_TRANSACTION_HASH = (
    os.getenv("BLOCKCHAIN_ETL_GETH_TRACE_BY_TRANSACTION_HASH") == "1"
)
GETH_TRACE_TRANSACTION_IGNORE_ERROR = (
    os.getenv("BLOCKCHAIN_ETL_GETH_TRACE_TRANSACTION_IGNORE_ERROR") == "1"
)
GETH_TRACE_IGNORE_ERROR_EMPTY_TRACE = (
    os.getenv("BLOCKCHAIN_ETL_GETH_TRACE_IGNORE_ERROR_EMPTY_TRACE") == "1"
)
GETH_TRACE_IGNORE_GASUSED_ERROR = (
    os.getenv("BLOCKCHAIN_ETL_GETH_TRACE_IGNORE_GASUSED_ERROR") == "1"
)
PARITY_TRACE_IGNORE_ERROR = os.getenv("BLOCKCHAIN_ETL_PARITY_TRACE_IGNORE_ERROR") == "1"

# FIXME: disable if this issue is resolved https://github.com/Fantom-foundation/go-opera/issues/218
IGNORE_PARENT_TRACE_MISSING = os.getenv("BLOCKCHAIN_ETL_IGNORE_TRACE_ERROR") == "1"

STORE_ERROR_IF_ENRICH_NOT_MATCHED_PATH = os.getenv(
    "BLOCKCHAIN_ETL_STORE_ERROR_IF_ENRICH_NOT_MATCHED_PATH"
)
IGNORE_ENRICH_NOT_MATCHED_ERROR = (
    os.getenv("BLOCKCHAIN_ETL_IGNORE_ENRICH_NOT_MATCHED_ERROR") == "1"
)

# special case for Arbitrum chain
IS_ARBITRUM_TRACE = os.getenv("BLOCKCHAIN_ETL_IS_ARBITRUM_TRACE") == "1"

# experimental: only for go-ethereum
IS_FLATCALL_TRACE = os.getenv("BLOCKCHAIN_ETL_IS_FLATCALL_TRACE") == "1"

# default 60s is enough, in some cases(eg: trace_block),
# we may need to increase the timeout to 180 or more seconds
REQUEST_TIMEOUT_SECONDS = int(os.getenv("BLOCKCHAIN_ETL_REQUEST_TIMEOUT_SECONDS", "60"))

SKIP_STREAM_IF_FAILED = os.getenv("BLOCKCHAIN_ETL_SKIP_STREAM_IF_FAILED") == "1"
SKIP_STREAM_SAVE_PATH = os.getenv("BLOCKCHAIN_ETL_SKIP_STREAM_SAVE_PATH")
