import os
import logging
import pymysql
from datetime import datetime

from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.misc.tidb import TiDBConnector
from blockchainetl.service import sql_temp
from bitcoinetl.service.btc_parser import BtcBlockParser, BtcUTXOParser


class FileItemExporter:
    def __init__(
        self,
        output_dir,
        tidb_url,
        export_block=True,
        export_tx=True,
        do_update=True,
        notify_callback=None,
    ):
        self._export_block = export_block
        self._export_tx = export_tx
        self._do_update = do_update
        self._notify_callback = notify_callback
        self._tidb = TiDBConnector(
            tidb_url,
            # set the max memory quota to 4GB, ref https://tinyurl.com/yjyx235k
            init_command="SET @@tidb_mem_quota_query = 4 << 30;",
        )

        # TODO: store block in date based csv file
        self._txs_dir = os.path.join(output_dir, "txs")
        os.makedirs(self._txs_dir, exist_ok=True)

    def open(self):
        pass

    def export_items(self, items):

        # the first element in `items` is the block struct,
        # the later are the transaction ones
        block = items[0]
        blknum = block.get("number")
        blk_st = block["timestamp"]
        utc = datetime.utcfromtimestamp(blk_st)
        st_day = utc.strftime("%Y-%m-%d")

        if self._export_block is True:
            self.export_block(st_day, block)

        if self._export_tx is True:
            self.export_txs(st_day, blknum, items[1:])

    def export_block(self, st_day, block):
        blknum = block["number"]
        row = BtcBlockParser(block).parse()

        def maybe_insert_block():
            with self._tidb.conn.cursor() as cursor:
                sql = "SELECT blknum FROM btc_blocks WHERE `blknum` = %s"
                cursor.execute(sql, (blknum,))
                result = cursor.fetchone()

                # already exists, skip
                if result is not None:
                    return

                cursor.execute(sql_temp.INSERT_BLOCK_SQL, row)

            self._tidb.conn.commit()

        try:
            maybe_insert_block()

        except (pymysql.OperationalError, pymysql.InterfaceError):
            self._tidb.reconnect(retry=5)
            maybe_insert_block()

        except pymysql.Error as e:
            msg = "INSERT INTO btc_blocks with st_day({}) block({}) failed: {}".format(
                st_day, blknum, e
            )
            logging.critical(msg)
            self._tidb.close()
            raise Exception(e)

    def export_txs(self, st_day, blknum, items):
        base = os.path.join(self._txs_dir, st_day)
        os.makedirs(base, exist_ok=True)
        txs_file = os.path.join(base, "{}.csv".format(blknum))
        with open(txs_file, "w") as tw:
            for item in items:
                if item["type"] != "transaction":
                    raise Exception(
                        "Unknown type: {} for item: {}".format(item["type"], item)
                    )
                self.export_tx(tw, item)
        self.load_file(txs_file)
        if self._do_update is True:
            self.update_tx(st_day, blknum)

        if self._notify_callback is not None:
            self._notify_callback(blknum, EntityType.TRANSACTION, txs_file)

    def export_tx(self, writer, item):
        for tx in BtcUTXOParser(item).parse():
            writer.write(tx + "\n")

    def load_file(self, file):
        return self._tidb.execute(
            sql_temp.to_loadcsv_sql(
                file,
                columns="_st, _st_day, blknum, txhash, txpos, isin, iscoinbase, pxhash, vin_idx, vin_cnt, vout_idx, vout_cnt, action_addr, action_value",
                table="btc_traces",
            ),
            "LOAD",
            file,
        )

    def update_tx(self, day_str, blknum):
        sql = sql_temp.UPDATE_UTXO_WITH_BLK_SQL.format(
            st_day=day_str, blknum=blknum, limit=sql_temp.DEFAULT_UPDATE_LIMIT
        )
        while True:
            rows = self._tidb.execute(sql, "UPDATE", day_str)
            if rows < sql_temp.DEFAULT_UPDATE_LIMIT:
                break

    def close(self):
        self._tidb.close()
