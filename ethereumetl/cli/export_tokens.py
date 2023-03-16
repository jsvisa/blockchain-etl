import logging
from datetime import datetime, timedelta

import click
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from web3 import Web3

from blockchainetl.file_utils import smart_open
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_contract_service import EthContractService


class ViaItem:
    FILE = "file"
    XFER = "xfer"
    TOKEN_XFER = "token_xfers"
    ERC1155_XFER = "erc1155_xfers"

    @classmethod
    def all_items(cls):
        return [ViaItem.FILE, ViaItem.XFER, ViaItem.TOKEN_XFER, ViaItem.ERC1155_XFER]


READ_TOKEN_TEMP = r"""
WITH tokens AS (
    SELECT DISTINCT
        address
    FROM
        {gp_schema}.tokens
)
SELECT
    token_address AS address
FROM
    {gp_schema}.{gp_table} A
LEFT JOIN tokens B ON
    A.token_address = B.address
WHERE
    _st_day = '{st_day}'
    AND B.address IS null
GROUP BY
    token_address
HAVING
    count(*) > {minimum_xfer_count}
"""

READ_CONTRACT_TEMP = r"""
SELECT
    address, _st, _st_day, blknum, txhash, txpos, trace_address
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY address ORDER BY _st DESC) AS _row
    FROM
        {gp_schema}.contracts
    WHERE
        address IN ({addresses})
) _
WHERE
    _row = 1
"""


def enrich_token_with_contract(gp_schema, pg_engine, tokens, web3: Web3):
    df = pd.DataFrame(tokens)
    df: pd.DataFrame = df[["address", "symbol", "name", "decimals", "total_supply"]]

    cs = EthContractService(web3)

    if pg_engine is not None:
        sql = READ_CONTRACT_TEMP.format(
            gp_schema=gp_schema, addresses="'" + "','".join(df["address"]) + "'"
        )
        contracts = pd.read_sql(sql, con=pg_engine)

        df = (
            df.merge(contracts, how="left", on=["address"])
            .assign(
                contract=lambda df_: df_.address.apply(cs.get_contract),
                is_erc20=lambda df_: df_.contract.apply(
                    lambda x: None if x is None else x.is_erc20
                ),
                is_erc721=lambda df_: df_.contract.apply(
                    lambda x: None if x is None else x.is_erc721
                ),
            )
            .drop(columns=["contract"])
        )

    # ethereum.tokens's primary key is (address, txhash)
    # so txhash can't be null, fill it with null
    # also we need to enrich the other fields
    if "txhash" not in df.columns:
        df["txhash"] = "0x"
    df.fillna(value={"txhash": "0x"}, inplace=True)

    df["source"] = "token_xfer"
    return df


def run_daily_etl(
    via,
    gp_schema,
    gp_engine,
    pg_engine,
    tokens,
    output,
    provider_uri,
    max_workers,
    load_into_db,
):
    logging.info(f"Export #{len(tokens)} tokens")

    if len(tokens) == 0:
        return

    web3 = Web3(get_provider_from_uri(provider_uri))
    exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN])
    job = ExportTokensJob(
        token_addresses_iterable=tokens,
        web3=ThreadLocalProxy(lambda: web3),
        item_exporter=exporter,
        max_workers=max_workers,
    )

    job.run()

    tokens = exporter.get_items(EntityType.TOKEN)
    df = pd.DataFrame(tokens)

    df = enrich_token_with_contract(gp_schema, pg_engine, tokens, web3)
    if via == ViaItem.ERC1155_XFER:
        df["is_erc1155"] = True

    if output is not None:
        df.to_csv(output, index=False)

    if load_into_db is True:
        df.to_sql(
            "tokens",
            schema=gp_schema,
            con=gp_engine,
            method="multi",
            if_exists="append",
            index=False,
            chunksize=500,
            dtype={"total_supply": sqlalchemy.sql.sqltypes.Numeric},
        )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--via",
    default="file",
    type=click.Choice(ViaItem.all_items()),
    help="Via token file, or from {chain}.token_xfers or {chain}.erc1155_xfers",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    help="The GreenPlum schema, default to the name of chain",
)
@click.option(
    "--gp-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum connection url, used to read {chain}.{token_xfers,erc1155_xfers}, "
    "and write back {chain}.tokens",
)
@click.option(
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_PG_URL",
    show_default=True,
    help="The PostgreSQL connection url, used to extract contracts",
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
    "-t",
    "--token-file",
    type=str,
    help="The file containing token addresses, one per line.",
)
@click.option(
    "-o",
    "--output",
    show_default=True,
    type=str,
    help="The output file, used to store the token in CSV format.",
)
@click.option(
    "-w",
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="The maximum number of workers.",
)
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the web3 provider",
)
@click.option(
    "--load-into-db",
    is_flag=True,
    default=False,
    show_default=True,
    help="Load the result csv into database or not.",
)
@click.option(
    "--minimum-xfer-count",
    type=int,
    default=0,
    show_default=True,
    help="Minimum transfer count(used if --via is ***xfer)",
)
def export_tokens(
    via,
    gp_schema,
    gp_url,
    pg_url,
    start_date,
    end_date,
    token_file,
    output,
    max_workers,
    provider_uri,
    load_into_db,
    minimum_xfer_count,
):
    """Export ERC20/ERC721/ERC1155 tokens via file,token_xfer,erc1155_xfer."""

    if via != ViaItem.FILE or load_into_db is True:
        assert None not in (
            gp_schema,
            gp_url,
        ), "--gp-schema/--gp-url should given"

    gp_engine, pg_engine = None, None
    if gp_url is not None:
        gp_engine = create_engine(gp_url)
    if pg_url is not None:
        pg_engine = create_engine(pg_url)

    if via != ViaItem.FILE:
        if via in (ViaItem.XFER, ViaItem.TOKEN_XFER):
            gp_table = "token_xfers"
        else:
            gp_table = "erc1155_xfers"

        for day in list(pd.date_range(start_date, end_date, inclusive="left")):
            st_day = day.date().strftime("%Y-%m-%d")
            sql = READ_TOKEN_TEMP.format(
                gp_schema=gp_schema,
                gp_table=gp_table,
                st_day=st_day,
                minimum_xfer_count=minimum_xfer_count,
            )
            logging.info(f"run {st_day}")
            df = pd.read_sql(sql, con=gp_engine)
            tokens = list(df["address"])
            run_daily_etl(
                via,
                gp_schema,
                gp_engine,
                pg_engine,
                tokens,
                output,
                provider_uri,
                max_workers,
                load_into_db,
            )

    else:
        assert token_file is not None, "--token-file should given"
        with smart_open(token_file, "r") as token_addresses_file:
            tokens = [token_address.strip() for token_address in token_addresses_file]
        run_daily_etl(
            via,
            gp_schema,
            gp_engine,
            pg_engine,
            tokens,
            output,
            provider_uri,
            max_workers,
            load_into_db,
        )
