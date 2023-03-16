import re
import logging
import pymysql
from time import sleep, time
from urllib.parse import urlparse


def my_connect(tidb, schema=None, cursorclass=pymysql.cursors.DictCursor, **kwargs):
    if cursorclass is None:
        cursorclass = pymysql.cursors.Cursor

    tidb = urlparse(tidb)
    if tidb.scheme != "mysql":
        raise Exception("args.tidb in wrong scheme")
    m = re.match("(.*):(.*)@(.*):(.*)", tidb.netloc)
    if m is None:
        raise Exception("args.tidb error: netloc")

    if not tidb.path.startswith("/"):
        raise Exception("args.tidb error: db not startswith '/'")

    # replace tidb path(DB) with custom schema
    mg = m.groups()
    db = schema or tidb.path[1:]

    logging.info(f"Connect to TiDB: mysql://{mg[0]}:***@{mg[2]}:{mg[3]}/{db}")

    my = pymysql.connect(
        host=mg[2],
        user=mg[0],
        password=mg[1],
        port=int(mg[3]),
        db=db,
        charset="utf8",
        local_infile=True,
        max_allowed_packet=64 * 1024 * 1024,
        cursorclass=cursorclass,
        **kwargs,
    )

    return my


class TiDBConnector:
    def __init__(self, tidb_url, tidb_schema=None, **kwargs):
        self._tidb = my_connect(tidb_url, tidb_schema, **kwargs)

    @property
    def conn(self):
        return self._tidb

    def reconnect(self, retry=3):
        connected = False
        for _i in range(retry):
            self._tidb.ping(reconnect=True)
            logging.info("TiDB reconnecting")
            if self._tidb.open:
                connected = True
                break
            else:
                sleep(_i * 2 + 3)
        if connected is False:
            raise Exception("Too many retried")

    def execute(self, sql, command, key):
        rows = 0
        with self._tidb.cursor() as cursor:
            st = time()
            logging.info(sql)
            try:
                cursor.execute(sql)
                self._tidb.commit()
                rows = self._tidb.affected_rows()
            except pymysql.OperationalError:
                self.reconnect(retry=3)

                cursor.execute(sql)
                self._tidb.commit()
                rows = self._tidb.affected_rows()
            except pymysql.Error as e:
                err = "execute {} {} failed: {}".format(command, key, e)
                logging.critical(err)
                raise Exception(err)
            finally:
                logging.info(
                    "finished execute {} with {} elapsed: {}ms, rows: {}".format(
                        command, key, int((time() - st) * 1000), rows
                    )
                )
        return rows

    def close(self):
        if self._tidb.open is True:
            self._tidb.close()
