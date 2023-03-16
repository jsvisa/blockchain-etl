LOAD_CSV_SQL = (
    "LOAD DATA LOCAL INFILE '__file__' INTO TABLE __table__ "
    "FIELDS TERMINATED BY '__terminated__' ENCLOSED BY '\"' "
    "LINES TERMINATED BY '\\n' IGNORE __ignorelines__ LINES (__columns__) "
)

DEFAULT_FIELD_TERMINATED = "^"
DEFAULT_UPDATE_LIMIT = 10000


def to_loadcsv_sql(
    file,
    columns,
    table,
    terminated=DEFAULT_FIELD_TERMINATED,
    ignore_lines=0,
):
    return (
        LOAD_CSV_SQL.replace("__file__", file)
        .replace("__columns__", columns)
        .replace("__table__", table)
        .replace("__terminated__", terminated)
        .replace("__ignorelines__", str(ignore_lines))
    )


UPDATE_UTXO_WITH_BLK_SQL = (
    "UPDATE btc_traces a JOIN btc_traces b ON "
    "a.pxhash = b.txhash "
    "AND a.vout_idx = b.vout_idx "
    "AND a.isin = true "
    "AND b.isin = false "
    "SET "
    "a.action_addr = b.action_addr, "
    "a.action_value = b.action_value "
    "WHERE "
    "a._st_day = '{st_day}' AND a.blknum = {blknum} AND a.action_addr = '' "
    "LIMIT {limit} "
)

UPDATE_UTXO_DAILY_SQL = (
    "UPDATE btc_traces a JOIN btc_traces b ON "
    "a.pxhash = b.txhash "
    "AND a.vout_idx = b.vout_idx "
    "AND a.isin = true "
    "AND b.isin = false "
    "SET "
    "a.action_addr = b.action_addr, "
    "a.action_value = b.action_value "
    "WHERE "
    "a._st_day = '{st_day}' AND a.action_addr = '' "
    "LIMIT {limit} "
)

DAILY_BLOCK_TXS_SQL = (
    "SELECT COALESCE(sum(num_txs), 0) AS cnt FROM btc_blocks WHERE _st_day = '{}' "
)

DAILY_TRACE_TXS_SQL = (
    "SELECT count(distinct txhash) AS cnt FROM btc_traces WHERE _st_day = '{}' "
)

DAILY_TRACE_EMP_SQL = (
    "SELECT count(*) AS cnt FROM btc_traces WHERE _st_day = '{}' AND action_addr = '' "
)

DFIND_TRACE_EMP_SQL = (
    "SELECT "
    "A.*, B.txpos "
    "FROM ( "
    "SELECT pxhash, vout_idx "
    "FROM btc_traces "
    "WHERE _st_day = '{st_day}' AND isin = true AND action_addr = '' "
    ") A "
    "LEFT JOIN ( "
    "SELECT txhash, txpos FROM btc_traces "
    ") B ON A.pxhash = B.txhash "
    "GROUP BY pxhash, vout_idx "
)

INSERT_MISSING_TRACE_SQL = (
    "INSERT INTO `btc_traces` ( "
    "_st, _st_day, blknum, txhash, pxhash, "
    "txpos, isin, iscoinbase, vin_idx, vin_cnt, "
    "vout_idx, vout_cnt, action_addr, action_value "
    ") VALUES ( "
    "%s, %s, %s, %s, %s, "
    "%s, %s, %s, %s, %s, "
    "%s, %s, %s, %s "
    ") "
)

INSERT_BLOCK_SQL = (
    "INSERT INTO `btc_blocks` ( "
    "_st, _st_day, blknum, blkhash, tx_count, "
    "blk_size, stripped_size, weight, version, nonce, "
    "bits "
    ") VALUES ( "
    "%s, %s, %s, %s, %s, "
    "%s, %s, %s, %s, %s, "
    "%s "
    ") "
)
