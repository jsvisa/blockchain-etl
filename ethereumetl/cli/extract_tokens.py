import os
import logging
import click
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from web3 import Web3
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.jobs.exporters.tokens_item_exporter import tokens_item_exporter
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_contract_service import EthContractService


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--gp-url",
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum conneciton url",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    help="The GreenPlum schema, default to the name of chain",
)
@click.option(
    "-s",
    "--start-date",
    default=(datetime.utcnow() - timedelta(days=1)).date(),
    show_default=True,
    help="Start datetime(included)",
)
@click.option(
    "-e",
    "--end-date",
    default=datetime.utcnow().date(),
    show_default=True,
    help="End datetime(excluded)",
)
@click.option(
    "-p",
    "--provider-uri",
    default="https://mainnet.infura.io",
    show_default=True,
    type=str,
    help="The URI of the web3 provider e.g. "
    "file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io",
)
@click.option(
    "-o",
    "--output-path",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_CACHE_PATH",
    type=click.Path(exists=True, dir_okay=True, writable=True),
    help="The output path.",
)
@click.option(
    "-w",
    "--max-workers",
    default=5,
    show_default=True,
    type=int,
    help="The maximum number of workers.",
)
@click.option(
    "--load-into-db",
    is_flag=True,
    default=False,
    show_default=True,
    help="Load the result csv into database or not.",
)
def extract_tokens(
    gp_url,
    gp_schema,
    start_date,
    end_date,
    provider_uri,
    output_path,
    max_workers,
    load_into_db,
):
    """Extract tokens from daily contracts."""

    web3 = ThreadLocalProxy(lambda: Web3(get_provider_from_uri(provider_uri)))
    engine = create_engine(gp_url)
    contract_service = EthContractService()

    for day in list(pd.date_range(start_date, end_date, inclusive="left")):
        st_day = day.date().strftime("%Y-%m-%d")
        df = pd.read_sql(
            f"""
SELECT
    *
FROM
    {gp_schema}.contracts
WHERE
    _st_day = '{st_day}'
    AND bytecode <> '0x'
    AND address NOT IN (SELECT DISTINCT address FROM {gp_schema}.tokens WHERE txhash <> '0x')
ORDER BY
    _st ASC
""",
            con=engine,
        )
        if df.empty:
            logging.info(f"Skip extract contracts @[{st_day}]")
            continue
        contracts = []
        for _, row in df.iterrows():
            bytecode = row["bytecode"]
            function_sighashes = row["func_sighashes"]
            if function_sighashes is not None:
                function_sighashes = (
                    function_sighashes.strip().lstrip("[").rstrip("]").split(",")
                )
                function_sighashes = [e for e in function_sighashes if len(e) > 0]
            if len(function_sighashes or []) == 0:
                function_sighashes = contract_service.get_function_sighashes(bytecode)
            is_erc20 = contract_service.is_erc20_contract(function_sighashes)
            is_erc721 = contract_service.is_erc721_contract(function_sighashes)
            contracts.append(
                {
                    "type": "contract",
                    "_st": row["_st"],
                    "_st_day": row["_st_day"],
                    "block_number": row["blknum"],
                    "transaction_hash": row["txhash"],
                    "transaction_index": row["txpos"],
                    "address": row["address"],
                    "creater": row["creater"],
                    "initcode": row["initcode"],
                    "bytecode": row["bytecode"],
                    "trace_type": row["trace_type"],
                    "trace_address": row["trace_address"],
                    "function_sighashes": function_sighashes,
                    "is_erc20": is_erc20,
                    "is_erc721": is_erc721,
                }
            )

        output = os.path.join(output_path, f"token-{st_day}.csv")

        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=web3,
            max_workers=max_workers,
            item_exporter=tokens_item_exporter(output, True),
        )

        job.run()

        logging.info(f"Finish extract #{df.shape[0]} contracts @[{st_day}]")

        if load_into_db is False:
            continue

        if os.stat(output).st_size == 0:
            continue

        df = pd.read_csv(output)
        df.rename(
            columns={
                "block_number": "blknum",
                "transaction_hash": "txhash",
                "transaction_index": "txpos",
            },
            inplace=True,
        )

        df.to_sql(
            "tokens",
            schema=gp_schema,
            con=engine,
            method="multi",
            if_exists="append",
            index=False,
            chunksize=5000,
        )
        logging.info(f"Finish INSERT #{df.shape[0]} tokens @[{st_day}]")
