from jinja2 import Template

READ_BLOCK_TEMPLATE = Template(
    """
SELECT
    min(block_timestamp) AS min_st,
    max(block_timestamp) AS max_st,
    sum(tx_count) AS block_txs
FROM (
    SELECT DISTINCT
        block_timestamp, blknum, tx_count
    FROM
        "{{chain}}".blocks
    WHERE
        blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
) _
    """
)

READ_TRACE_TEMPLATE = Template(
    """
SELECT DISTINCT
    _st, _st_day, blknum, txhash, txpos, from_address, to_address,
    (CASE status WHEN 1 THEN round(value) ELSE 0 END)::text AS value,
    trace_address, status
FROM
    "{{chain}}".traces
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
    AND trace_type <> 'reward' -- use block_rewards instead
    AND (call_type is null OR call_type NOT IN ('callcode', 'staticcall', 'delegatecall'))
    AND from_address is not null
    AND to_address is not null
    AND trace_address <> '[]'

UNION ALL

SELECT DISTINCT
    _st, _st_day, blknum, txhash, txpos, from_address,
    COALESCE(to_address, receipt_contract_address) AS to_address,
    round(CASE receipt_status WHEN null THEN value WHEN 1 THEN value ELSE 0 END)::text AS value,
    '[]'::text AS trace_address, receipt_status AS status
FROM
    "{{chain}}".txs
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
    AND from_address is not null
"""
)

READ_TX_AS_TRACE_TEMPLATE = Template(
    """
SELECT DISTINCT
    _st, _st_day, blknum, txhash, txpos, from_address,
    COALESCE(to_address, receipt_contract_address) AS to_address,
    round(CASE receipt_status WHEN null THEN value WHEN 1 THEN value ELSE 0 END)::text AS value,
    receipt_status AS status
FROM
    "{{chain}}".txs
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
    AND from_address is not null
"""
)

READ_TX_TEMPLATE = Template(
    """
SELECT DISTINCT
    _st, _st_day, blknum, txhash, txpos, from_address, to_address,
    round(value)::text AS value,
    round(
        receipt_gas_used::numeric
        *
        COALESCE(receipt_effective_gas_price, gas_price)::numeric
    )::text AS fee_value
FROM
    "{{chain}}".txs
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
{%- if chain == 'cronos' %}
    AND receipt_gas_used is not null
{%- endif -%}
"""
)

READ_FULL_TRACE_TEMPLATE = Template(
    """
WITH txs AS (
    SELECT DISTINCT
        _st, _st_day, blknum, txhash, txpos, from_address, to_address, value,
        receipt_gas_used AS gas_used, receipt_effective_gas_price AS gas_price
    FROM
        "{{chain}}".txs
    WHERE
        block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
        AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
        AND txhash is not null
),
traces AS (
    SELECT DISTINCT
        _st, _st_day, blknum, txhash, txpos, from_address, to_address, value, trace_address, status
    FROM
        "{{chain}}".traces
    WHERE
        block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
        AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
        AND trace_type <> 'reward' -- use block_rewards instead
        AND (call_type is null OR call_type NOT IN ('callcode', 'staticcall', 'delegatecall'))
),
block_rewards AS (
    SELECT
        *
    FROM
        "{{chain}}".block_rewards
    WHERE
        _st_day IN (
            {%- for d in dates -%}'{{d}}'{%- if not loop.last -%}, {% endif -%}{%- endfor -%}
        )
        AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
),
miner_stats AS (
    SELECT
        _st,
        _st_day,
        blknum,
        miner AS address,
        reward AS value
    FROM
        block_rewards
    UNION ALL
    SELECT
        _st,
        _st_day,
        blknum,
        uncle0_miner AS address,
        uncle0_reward AS value
    FROM
        block_rewards
    UNION ALL
    SELECT
        _st,
        _st_day,
        blknum,
        uncle1_miner AS address,
        uncle1_reward AS value
    FROM
        block_rewards
),
cnb_stats AS (
    SELECT
        address AS cnb_address,
        count(distinct blknum) AS cnb_blocks,
        count(*) AS cnb_txs,
        count(*) AS cnb_xfers,
        min(_st) AS cnb_1th_st,
        max(_st) AS cnb_nth_st,
        sum(value) AS cnb_value,
        min(blknum) AS cnb_1th_blknum,
        max(blknum) AS cnb_nth_blknum,
        min(_st_day) AS cnb_1th_st_day,
        max(_st_day) AS cnb_nth_st_day
    FROM
        miner_stats
    GROUP BY
        1
),
vin_stats AS (
    SELECT
        to_address AS vin_address,
        count(distinct blknum) AS vin_blocks,
        count(distinct txhash) AS vin_txs,
        count(*) AS vin_xfers,
        sum(CASE status WHEN 1 THEN value ELSE 0 END) AS vin_value,
        min(_st) AS vin_1th_st,
        max(_st) AS vin_nth_st,
        min(blknum) AS vin_1th_blknum,
        max(blknum) AS vin_nth_blknum,
        min(_st_day) AS vin_1th_st_day,
        max(_st_day) AS vin_nth_st_day
    FROM
        traces
    GROUP BY
        1
),
fee_stats AS (
    SELECT
        from_address AS out_address,
        sum(gas_used * gas_price::numeric) AS fee_value
    FROM
        txs
    GROUP BY
        1
),
out_stats AS (
    SELECT
        A.from_address AS out_address,
        count(distinct A.blknum) AS out_blocks,
        count(distinct A.txhash) AS out_txs,
        count(*) AS out_xfers,
        sum(CASE A.status WHEN 1 THEN A.value ELSE 0 END) AS out_value,
        min(A._st) AS out_1th_st,
        max(A._st) AS out_nth_st,
        min(A.blknum) AS out_1th_blknum,
        max(A.blknum) AS out_nth_blknum,
        min(A._st_day) AS out_1th_st_day,
        max(A._st_day) AS out_nth_st_day
    FROM
        traces A
    LEFT JOIN txs B ON
        A.txhash = B.txhash
    GROUP BY
        1
),
inout_flows AS (
    SELECT
        COALESCE(A.cnb_address, B.vin_address, C.out_address) AS address,
        A.*,
        B.*,
        C.*,
        D.fee_value
    FROM
        cnb_stats A
    FULL JOIN vin_stats B ON A.cnb_address = B.vin_address
    FULL JOIN out_stats C ON A.cnb_address = C.out_address
    LEFT JOIN fee_stats D ON C.out_address = D.out_address
)

-- FULL JOIN may return multi columns for the same address(_st_day maybe null)
SELECT
    address, blknum,
    out_blocks, vin_blocks, cnb_blocks,
    out_txs, vin_txs, cnb_txs,
    out_xfers, vin_xfers, cnb_xfers,
    out_value::TEXT AS out_value,
    vin_value::TEXT AS vin_value,
    cnb_value::TEXT AS cnb_value,
    fee_value::TEXT AS fee_value,
    out_1th_st, vin_1th_st, cnb_1th_st, out_nth_st, vin_nth_st, cnb_nth_st,
    out_1th_blknum, vin_1th_blknum, cnb_1th_blknum, out_nth_blknum, vin_nth_blknum, cnb_nth_blknum,
    out_1th_st_day, vin_1th_st_day, cnb_1th_st_day, out_nth_st_day, vin_nth_st_day, cnb_nth_st_day,
    (vin_value + cnb_value - out_value - fee_value)::TEXT AS value
FROM (
    SELECT
        address,
        {{et_blknum}} AS blknum,

        sum(COALESCE(out_blocks, 0))::BIGINT AS out_blocks,
        sum(COALESCE(vin_blocks, 0))::BIGINT AS vin_blocks,
        sum(COALESCE(cnb_blocks, 0))::BIGINT AS cnb_blocks,

        sum(COALESCE(out_txs, 0))::BIGINT AS out_txs,
        sum(COALESCE(vin_txs, 0))::BIGINT AS vin_txs,
        sum(COALESCE(cnb_txs, 0))::BIGINT AS cnb_txs,

        sum(COALESCE(out_xfers, 0))::BIGINT AS out_xfers,
        sum(COALESCE(vin_xfers, 0))::BIGINT AS vin_xfers,
        sum(COALESCE(cnb_xfers, 0))::BIGINT AS cnb_xfers,

        sum(COALESCE(out_value, 0))::NUMERIC AS out_value,
        sum(COALESCE(vin_value, 0))::NUMERIC AS vin_value,
        sum(COALESCE(cnb_value, 0))::NUMERIC AS cnb_value,
        sum(COALESCE(fee_value, 0))::NUMERIC AS fee_value,

        min(out_1th_st) AS out_1th_st,
        min(vin_1th_st) AS vin_1th_st,
        min(cnb_1th_st) AS cnb_1th_st,
        max(out_nth_st) AS out_nth_st,
        max(vin_nth_st) AS vin_nth_st,
        max(cnb_nth_st) AS cnb_nth_st,

        min(out_1th_blknum) AS out_1th_blknum,
        min(vin_1th_blknum) AS vin_1th_blknum,
        min(cnb_1th_blknum) AS cnb_1th_blknum,
        max(out_nth_blknum) AS out_nth_blknum,
        max(vin_nth_blknum) AS vin_nth_blknum,
        max(cnb_nth_blknum) AS cnb_nth_blknum,

        min(out_1th_st_day) AS out_1th_st_day,
        min(vin_1th_st_day) AS vin_1th_st_day,
        min(cnb_1th_st_day) AS cnb_1th_st_day,
        max(out_nth_st_day) AS out_nth_st_day,
        max(vin_nth_st_day) AS vin_nth_st_day,
        max(cnb_nth_st_day) AS cnb_nth_st_day
    FROM
        inout_flows
    WHERE
        address is not null -- in case the block reward data is missing
    GROUP BY
        1
) _
    """
)

READ_LOG_TEMPLATE = Template(
    """
SELECT DISTINCT
    _st, _st_day, blknum, txhash, txpos, logpos, address, topics, data
FROM
    "{{chain}}".logs
WHERE
    block_timestamp >= '{{st_timestamp}}' AND block_timestamp <= '{{et_timestamp}}'
    AND blknum >= {{st_blknum}} AND blknum <= {{et_blknum}}
{% if topics %}
    AND split_part(topics, ',', 1) IN (
        {%- for t in topics -%}'{{t}}'{%- if not loop.last -%}, {% endif -%}{%- endfor -%}
    )
{% endif %}
{% if addresses %}
    AND address IN (
        {%- for a in addresses -%}'{{a}}'{%- if not loop.last -%}, {% endif -%}{%- endfor -%}
    )
{% endif %}
"""
)
