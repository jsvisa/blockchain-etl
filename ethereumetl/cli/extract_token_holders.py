import os
import logging
from datetime import datetime, timedelta
from time import time

import click

from blockchainetl.utils import time_elapsed
from blockchainetl.cli.utils import global_click_options
from blockchainetl.service.redis_stream_service import RedisStreamService
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.extract_token_holders_job import ExtractTokenHoldersJob
from ethereumetl.cli.utils import validate_and_read_tokens


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@global_click_options
@click.option(
    "--gp-url",
    "--pg-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_GP_URL",
    help="The GreenPlum connection url",
)
@click.option(
    "--gp-schema",
    type=str,
    default=None,
    help="The GreenPlum schema, default to the name of chain",
)
@click.option(
    "-s",
    "--st-day",
    default=str((datetime.utcnow() - timedelta(days=1)).date()),
    type=click.DateTime(["%Y-%m-%d"]),
    show_default=True,
    help="Which day",
)
@click.option(
    "-o",
    "--output",
    required=True,
    show_default=True,
    type=str,
    help="The output dir. If not specified /token_address is used.",
)
@click.option(
    "--cache-path",
    required=True,
    show_default=True,
    type=str,
    help="The cache dir.",
)
@click.option(
    "--tokens-type",
    show_default=True,
    default="erc20,erc721",
    required=True,
    help="The token type to export",
)
@click.option(
    "--tokens-file",
    show_default=True,
    type=click.Path(exists=True, file_okay=True, readable=True),
    default="etc/eth_tokens.yaml",
    required=True,
    help="The file contains token address",
)
@click.option(
    "-b",
    "--batch-size",
    default=5000,
    show_default=True,
    type=int,
    help="How many addresses extracted into one file",
)
@click.option(
    "--limit",
    show_default=True,
    type=int,
    default=None,
    help="The maximum number of records to collect, used for debug",
)
@click.option(
    "--enable-notify",
    is_flag=True,
    show_default=True,
    help="Notify into redis stream or not",
)
@click.option(
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/4",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_REDIS_URL",
    help="The Redis conneciton url",
)
@click.option(
    "--redis-stream-prefix",
    type=str,
    default="extract-stream-",
    show_default=True,
    help="The Redis stream used to store notify messages.(Put behind the chain)",
)
@click.option(
    "--redis-result-prefix",
    type=str,
    default="extract-result-",
    show_default=True,
    help="The Redis result sorted set used to store thee dumped block.(Put behind the chain)",
)
@click.option(
    "--only-address",
    type=str,
    show_default=True,
    help="Run only those addresses",
)
@click.option(
    "--file-server-url",
    type=str,
    show_default=True,
    help="Save file into local path, and expose under a web server",
)
@click.option(
    "--file-server-prefix",
    type=str,
    show_default=True,
    help="File server prefix, need to be replaced",
)
def extract_token_holders(
    chain,
    gp_url,
    gp_schema,
    st_day,
    output,
    cache_path,
    tokens_type,
    tokens_file,
    batch_size,
    limit,
    enable_notify,
    redis_url,
    redis_stream_prefix,
    redis_result_prefix,
    only_address,
    file_server_url,
    file_server_prefix,
):
    """
        Extract ERC20/ERC721/ERC1155 token pairs from Greenplum/Postgres'
        {chain}.addr_tokens into CSV(with redis notify).

        # extract tokens from Postgres like below:

    SELECT
        token_address AS address,
        row_number() OVER (order by cmc_rank asc) AS id,
        symbol AS name,
        cmc_rank
    FROM (
        SELECT DISTINCT ON (platform_token_address)
            symbol,
            cmc_rank,
            platform_token_address AS token_address
        FROM
            public.cmc_listings
        WHERE
            platform_slug = 'ethereum'
            AND last_updated >= CURRENT_DATE - 30
            AND platform_token_address <> '0x0000000000000000000000000000000000000000'
            AND cmc_rank < 500
    ) _ ORDER BY
        cmc_rank ASC;
    """

    gp_schema = gp_schema or chain
    token_types = tokens_type.split(",")

    if limit is not None:
        batch_size = min(batch_size, limit)

    st_day = st_day.strftime("%Y-%m-%d")
    output_path = os.path.join(output, st_day)

    full_tokens = validate_and_read_tokens(tokens_file)
    required_attrs = {"id", "name", "address"}
    for kind, tokens in full_tokens.items():
        if kind not in ("erc20_tokens", "erc721_tokens") or tokens is None:
            continue

        invalid = [e for e in tokens if not required_attrs < set(e.keys())]
        if len(invalid) > 0:
            raise ValueError(
                f"token file of kind: {kind} with {invalid} "
                f"missing required attributes: {required_attrs}"
            )

    logging.info(
        f"Extract @{st_day} of types: {token_types} "
        f"tokens: {[(e, len(full_tokens[e])) for e in full_tokens]}"
    )

    erc20_tokens = []
    if "erc20" in token_types:
        erc20_tokens = full_tokens.get("erc20_tokens", [])

    erc721_tokens = []
    if "erc721" in token_types:
        erc721_tokens = full_tokens.get("erc721_tokens", [])

    def output_path_rewrite(x):
        if file_server_url is None:
            return x
        return os.path.join(
            file_server_url, x.replace(file_server_prefix, "").lstrip("/")
        )

    def job(token, kind="erc20", idx=None, total=None):
        st = time()
        notify = None
        if enable_notify is True:
            # this stream is named as extract-xxx, not export-xxx
            notify = RedisStreamService(
                redis_url, [EntityType.TOKEN_HOLDER]
            ).create_notify(
                chain,
                redis_stream_prefix,
                redis_result_prefix,
            )

        name = token["name"]
        address = token["address"].lower()
        logging.info(f"start extract token-holder({idx}/{total}): {name}")
        job = ExtractTokenHoldersJob(
            gp_url,
            gp_schema,
            kind,
            name,
            token["id"],
            address,
            st_day=st_day,
            output_path=output_path,
            cache_path=cache_path,
            batch_size=batch_size,
            limit=limit,
            notify_callback=notify,
            token_genesis_blknum=token.get("created_blknum"),
            output_path_rewrite=output_path_rewrite,
        )
        job.run()
        logging.info(
            f"finish extract token-holder({idx}/{total}): {name} elapsed: {time_elapsed(st)}"
        )

    if only_address is not None:
        dev_addresses = set(e.lower() for e in (only_address or "").split(","))
        erc20_tokens = [
            t for t in erc20_tokens if t["address"].lower() in dev_addresses
        ]
        erc721_tokens = [
            t for t in erc721_tokens if t["address"].lower() in dev_addresses
        ]

    n_tokens = len(erc20_tokens) + len(erc721_tokens)
    for idx, token in enumerate(erc20_tokens):
        job(token, "erc20", idx, n_tokens)
    for idx, token in enumerate(erc721_tokens):
        job(token, "erc721", idx, n_tokens)

    logging.info("finish extract all token-holders")
