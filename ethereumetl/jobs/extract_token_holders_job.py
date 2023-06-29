import os
import shutil
import logging
from subprocess import check_output
import concurrent.futures

try:
    import polars as pl
except Exception:
    import pandas as pl
import psycopg2

from blockchainetl.streaming.postgres_utils import copy_into_csv_file
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.base_job import BaseJob

TOKEN_TYPE_IDS = {
    "erc20": "0",
    "erc721": "1",
    "erc1155": "2",
}


class ExtractTokenHoldersJob(BaseJob):
    def __init__(
        self,
        gp_url: str,
        gp_schema: str,
        token_type: str,
        token_name: str,
        token_id: int,
        token_address: str,
        st_day: str,
        output_path: str,
        cache_path: str,
        batch_size: int,
        limit: int,
        token_genesis_blknum: int = None,
        notify_callback=None,
        output_path_rewrite=None,
    ):
        self.conn = psycopg2.connect(gp_url)
        self.gp_schema = gp_schema
        self.token_type = token_type
        self.token_type_id = TOKEN_TYPE_IDS[token_type]
        self.token_name = token_name
        self.token_id = token_id
        self.token_address = token_address
        self.st_day = int(st_day.replace("-", ""))

        self.batch_size = batch_size
        self.limit = limit

        self.output_path = os.path.join(output_path, token_address)
        self.output_path_rewrite = output_path_rewrite
        self.token_cache_file = os.path.join(cache_path, st_day, f"{token_address}.csv")
        self.notify_callback = notify_callback
        self.pages = 0
        self.max_id = 0
        self.n_address = 0
        self.blknum = 0
        self.token_genesis_blknum = token_genesis_blknum

    def _start(self):
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(os.path.dirname(self.token_cache_file), exist_ok=True)

        with self.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT max(blknum) AS blknum FROM {self.gp_schema}.blocks "
                f"WHERE _st_day = '{self.st_day}'",
            )
            row = cursor.fetchone()
            assert row is not None and len(row) == 1
            self.blknum = row[0]

    def _export(self):
        if self.token_genesis_blknum and self.token_genesis_blknum >= self.blknum:
            logging.warning(
                f"token created at block: {self.token_genesis_blknum} > "
                f"current block: {self.blknum}"
            )
            return

        self._export_full()

    def _export_full(self):
        if self.token_type in {"erc20", "erc721"}:
            sql = f"""
            SELECT
                address
            FROM
                {self.gp_schema}.addr_tokens
            WHERE
                token_address = '{self.token_address}'
            """
        else:
            raise NotImplementedError

        token_file = self.token_cache_file
        if os.path.exists(token_file) and os.path.isfile(token_file):
            # minus the header line
            logging.info(f"token file {token_file} exists, skip postgres load")
            self.n_address = int(check_output(["wc", "-l", token_file]).split()[0]) - 1
        else:
            self.n_address = copy_into_csv_file(
                self.conn, sql, self.token_cache_file, delimiter=","
            )

        if self.n_address <= 0:
            logging.error(f"no address found for token: {self.token_address}")
            return

        # simple copy file and notify
        if self.n_address <= self.batch_size:
            self.n_page = 1
            file_name = f"{self.output_path}/0.csv"
            shutil.copy(token_file, file_name)
            self._maybe_notify_page(0, file_name)
            return

        n_page = self.n_address // self.batch_size
        if self.n_address % self.batch_size > 0:
            n_page += 1

        self.n_page = n_page

        df = pl.read_csv(token_file, has_header=True)
        with concurrent.futures.ThreadPoolExecutor(10) as executor:
            futures = {}
            for page in range(0, n_page):
                start = page * self.batch_size
                end = min(start + self.batch_size, df.shape[0])
                futures[executor.submit(self._save_page, page, df[start:end])] = (
                    start,
                    end,
                )
            for future in concurrent.futures.as_completed(futures):
                logging.info(f"finish part: {futures[future]}")

    def _save_page(self, page, df: pl.DataFrame):
        file_name = f"{self.output_path}/{page}.csv"
        logging.info(
            f"save token: {self.token_name} with page {page}/{self.n_page} to {file_name}"
        )

        # pl.DataFrame save into file directly
        df.to_csv(file_name, has_header=True, sep=",")
        self._maybe_notify_page(page, file_name)

    def _maybe_notify_page(self, page, file_name):
        if self.notify_callback is not None:
            self.notify_callback(
                self._fmt_blknum_key(page),
                EntityType.TOKEN_HOLDER,
                self._fmt_blknum_val(page, file_name),
            )

    def _fmt_blknum_key(self, page):
        return int(f"{self.st_day}{self.token_type_id}{self.token_id:08d}{page:010d}")

    def _fmt_blknum_val(self, page, file):
        if self.output_path_rewrite is not None:
            file = self.output_path_rewrite(file)

        return "^".join(
            str(e)
            for e in [
                self.blknum,
                self.token_address,
                self.n_page,
                self.n_address,
                page,
                file,
            ]
        )

    def _end(self):
        self.conn.close()
