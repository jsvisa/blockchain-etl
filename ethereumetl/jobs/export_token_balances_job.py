import os
import logging
from typing import Dict
from datetime import datetime

import pandas as pd
from web3 import Web3
from multicall import Call
from multicall.service.ether_service import EtherService

from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.misc.pd_write_file import save_df_into_file, DEFAULT_FIELD_TERMINATED
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.enumeration.column_type import ColumnType


class ExportTokenBalancesJob(BaseJob):
    _st = None
    _st_day = None
    _ignore_error = True  # TODO: if erc20, set to False

    def __init__(
        self,
        web3_provider: Web3,
        ether_service: EtherService,
        job_key,
        job_item,
        output_dir,
        batch_size,
        notify_callback=None,
    ):
        # see jobs/extract_token_holders_job.py#_fmt_blknum_val(self, page, file)
        blknum, token_address, n_page, n_address, page, file = job_item.split("^")
        self.blknum = int(blknum)
        self.token_address = token_address
        self.n_page = int(n_page)
        self.n_address = int(n_address)
        self.page = int(page)
        self.input_file = file

        self.job_key = job_key
        self.output_dir = output_dir
        self.total_supply = 0
        self.batch_size = batch_size
        self.web3 = web3_provider
        self.ether_service = ether_service

        self.ct = ColumnType()
        self.notify_callback = notify_callback

    def _start(self):
        call = Call(
            target=self.token_address,
            function="totalSupply()(uint256)",
            request_id="total_supply",
            block_id=self.blknum,
            ignore_error=True,
        )
        result: Dict = self.ether_service.mc.agg(
            [call], as_dict=True, ignore_error=True
        )
        self.total_supply = result["total_supply"]
        block = self.web3.eth.get_block(self.blknum)
        self._st = block.timestamp
        self._st_day = datetime.utcfromtimestamp(block.timestamp).strftime("%Y-%m-%d")
        self.output_dir = os.path.join(
            self.output_dir, self._st_day, EntityType.TOKEN_BALANCE, self.token_address
        )
        os.makedirs(self.output_dir, exist_ok=True)

    def _export(self):
        addr_df = pd.read_csv(self.input_file, sep=DEFAULT_FIELD_TERMINATED)
        if addr_df.empty:
            logging.warning(f"file {self.input_file} is empty")
            return

        balances = self.ether_service.get_token_balance(
            self.token_address,
            accounts=addr_df["address"],
            block_id=self.blknum,
            batch_size=self.batch_size,
            max_workers=100,
            ignore_error=self._ignore_error,
        )
        if len(balances) == 0:
            logging.info(f"balaces of {self.input_file} is empty")
            # FIXME: In this case, how do we make sure the data is complete when we check it
            return

        df = pd.DataFrame(balances)
        df.rename(
            columns={
                "account": "address",
                "token": "token_address",
                "value": "balance",
            },
            inplace=True,
        )
        df["_st"] = self._st
        df["_st_day"] = self._st_day
        df["blknum"] = self.blknum
        df["total_supply"] = self.total_supply
        df["ranking"] = None
        df["page"] = self.page
        df["page_holder"] = len(balances)
        df["n_page"] = self.n_page
        df["n_address"] = self.n_address

        outfile = f"{self.output_dir}/{self.blknum}-{self.page}.csv"
        logging.info(f"write token-balance #{df.shape[0]} to {outfile}")
        save_df_into_file(
            df,
            outfile,
            columns=self.ct[EntityType.TOKEN_BALANCE],
            types=self.ct.astype(EntityType.TOKEN_BALANCE),
            entity_type=EntityType.TOKEN_BALANCE,
        )

        if self.notify_callback is not None:
            self.notify_callback(self.job_key, EntityType.TOKEN_BALANCE, outfile)
