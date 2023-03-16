from jinja2 import Template

READ_BLOCK_TEMPLATE = Template(
    """
SELECT
    'block'                     AS type,
    block_timestamp             AS timestamp,
    blknum                      AS number,
    blkhash                     AS hash,
    parent_hash                 AS parent_hash,
    nonce                       AS nonce,
    sha3_uncles                 AS sha3_uncles,
    logs_bloom                  AS logs_bloom,
    txs_root                    AS transactions_root,
    state_root                  AS state_root,
    receipts_root               AS receipts_root,
    miner                       AS miner,
    difficulty                  AS difficulty,
    total_difficulty            AS total_difficulty,
    blk_size                    AS size,
    extra_data                  AS extra_data,
    gas_limit                   AS gas_limit,
    gas_used                    AS gas_used,
    tx_count                    AS transaction_count,
    base_fee_per_gas            AS base_fee_per_gas,
    uncle_count                 AS uncle_count,
    uncle0_hash                 AS uncle0_hash,
    uncle1_hash                 AS uncle1_hash
FROM
    {{schema}}.blocks
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
ORDER BY
    blknum ASC
    """
)

READ_TX_TEMPLATE = Template(
    """
SELECT
    'transaction'               AS type,
    block_timestamp             AS block_timestamp,
    blknum                      AS block_number,
    txhash                      AS hash,
    nonce                       AS nonce,
    txpos                       AS transaction_index,
    from_address                AS from_address,
    to_address                  AS to_address,
    value                       AS value,
    gas                         AS gas,
    gas_price                   AS gas_price,
    input                       AS input,
    max_fee_per_gas             AS max_fee_per_gas,
    max_priority_fee_per_gas    AS max_priority_fee_per_gas,
    tx_type                     AS transaction_type,
    receipt_cumulative_gas_used AS receipt_cumulative_gas_used,
    receipt_gas_used            AS receipt_gas_used,
    receipt_contract_address    AS receipt_contract_address,
    receipt_root                AS receipt_root,
    receipt_status              AS receipt_status,
    receipt_effective_gas_price AS receipt_effective_gas_price,
    receipt_log_count           AS receipt_log_count
FROM
    {{schema}}.txs
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
ORDER BY
    blknum ASC, txpos ASC
"""
)

READ_TRACE_TEMPLATE = Template(
    """
SELECT
    'trace'                     AS type,
    block_timestamp             AS block_timestamp,
    blknum                      AS block_number,
    txhash                      AS transaction_hash,
    txpos                       AS transaction_index,
    from_address                AS from_address,
    to_address                  AS to_address,
    value                       AS value,
    input                       AS input,
    output                      AS output,
    trace_type                  AS trace_type,
    call_type                   AS call_type,
    reward_type                 AS reward_type,
    gas                         AS gas,
    gas_used                    AS gas_used,
    subtraces                   AS subtraces,
    trace_address               AS trace_address,
    error                       AS error,
    status                      AS status
FROM
    {{schema}}.traces
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
ORDER BY
    blknum ASC, txpos ASC, trace_address ASC
"""
)

READ_LOG_TEMPLATE = Template(
    """
SELECT
    'log'                       AS type,
    block_timestamp             AS block_timestamp,
    blknum                      AS block_number,
    txhash                      AS transaction_hash,
    txpos                       AS transaction_index,
    logpos                      AS log_index,
    address                     AS address,
    topics                      AS topics,
    split_part(topics, ',', 1)  AS topic0,
    split_part(topics, ',', 2)  AS topic1,
    split_part(topics, ',', 3)  AS topic2,
    split_part(topics, ',', 4)  AS topic3,
    data                        AS data
FROM
    {{schema}}.logs
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
ORDER BY
    blknum ASC, logpos ASC
"""
)

READ_TOKEN_XFER_TEMPLATE = Template(
    """
SELECT
    'token_xfer'                AS type,
    block_timestamp             AS block_timestamp,
    blknum                      AS block_number,
    txhash                      AS transaction_hash,
    txpos                       AS transaction_index,
    logpos                      AS log_index,
    token_address               AS token_address,
    from_address                AS from_address,
    to_address                  AS to_address,
    value                       AS value,
    COALESCE(symbol, name)      AS name,
    decimals                    AS decimals
FROM
    {{schema}}.token_xfers
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
ORDER BY
    blknum ASC, logpos ASC
"""
)
