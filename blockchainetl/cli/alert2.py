import logging

import click
from web3 import Web3, HTTPProvider
from sqlalchemy import create_engine

from blockchainetl.alert import rule_udf
from blockchainetl.alert.rule_set import RuleSets
from blockchainetl.cli.utils import evm_chain_options, pick_random_provider_uri
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.alert_exporter import AlertExporter
from blockchainetl.service.label_service import LabelService
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.streaming.eth_alert_adapter import EthAlertAdapter
from blockchainetl.service.price_service import PriceService


@click.command(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@evm_chain_options
@click.option(
    "-l",
    "--last-synced-block-file",
    default=".priv/last_alerted_block.txt",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_LAST_ALERTFILE",
    type=str,
    help="The file with the last synced block number.",
)
@click.option(
    "--db-url",
    type=str,
    envvar="BLOCKCHAIN_ETL_PG_URL",
    required=True,
    help="The database connection url",
)
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "--price-url",
    type=str,
    required=True,
    help="The price connection url, used in price service",
)
@click.option(
    "--price-apikey",
    type=str,
    envvar="BLOCKCHAIN_ETL_PRICE_SERVICE_API_KEY",
    help="The price service api key",
)
@click.option(
    "-s",
    "--start-block",
    default=None,
    show_default=True,
    type=int,
    help="Start block",
)
@click.option(
    "-e",
    "--end-block",
    default=None,
    show_default=True,
    type=int,
    help="End block",
)
@click.option(
    "-E",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_ALERT),
    show_default=True,
    type=str,
    help="The list of entity types to alert.",
)
@click.option(
    "--rule-id",
    type=str,
    show_default=True,
    help="Run with this rule id, used to debug the rule expression",
)
@click.option(
    "--rule-file",
    type=click.Path(exists=True, readable=True, file_okay=True),
    default="etc/rules.yaml",
    show_default=True,
    envvar="BLOCKCHAIN_ETL_ALERT_RULEFILE",
    help="The rule file in YAML",
)
@click.option(
    "--rule-variable-dir",
    type=click.Path(exists=True, readable=True, dir_okay=True),
    default="etc/variables",
    show_default=True,
    help="The rule variable directory",
)
@click.option(
    "--period-seconds",
    default=3,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "-b",
    "--batch-size",
    default=50,
    show_default=True,
    type=int,
    help="How many query items are carried in a JSON RPC request, "
    "the JSON RPC Server is required to support batch requests",
)
@click.option(
    "-w",
    "--max-workers",
    default=5,
    show_default=True,
    type=int,
    help="The number of workers",
)
@click.option(
    "--pending-mode",
    is_flag=True,
    default=False,
    show_default=True,
    help="In pending mode, blocks are not finalized",
)
@click.option(
    "--label-service-url",
    type=str,
    default=None,
    envvar="BLOCKCHAIN_ETL_LABEL_SERVICE_URL",
    help="LabelService URL, used to fetch address labels",
)
def alert2(
    chain,
    last_synced_block_file,
    db_url,
    provider_uri,
    price_url,
    price_apikey,
    start_block,
    end_block,
    entity_types,
    rule_id,
    rule_file,
    rule_variable_dir,
    period_seconds,
    batch_size,
    max_workers,
    pending_mode,
    label_service_url,
):
    """Read items from PostgreSQL and alert with rules"""
    # rule udf hack.
    rule_udf.ALL["label_of"] = rule_udf.label_of(
        label_service_url, chain, "addr_labels"
    )

    entity_types = parse_entity_types(entity_types)

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    # load rulesets from file
    RuleSets.load(rule_file, rule_variable_dir)
    RuleSets.open()

    ruleset = RuleSets()[chain]
    receivers = RuleSets.receivers
    engine = create_engine(
        db_url,
        pool_size=max_workers,
        max_overflow=max_workers + 10,
        pool_recycle=3600,
    )

    token_service = None
    if chain in Chain.ALL_ETHEREUM_FORKS:
        web3 = Web3(HTTPProvider(provider_uri))
        token_service = EthTokenService(web3)
    price_service = PriceService(price_url, price_apikey)

    alert_exporter = AlertExporter(
        chain,
        ruleset,
        receivers,
        rule_id=rule_id,
        max_workers=max_workers,
        token_service=token_service,
        price_service=price_service,
        label_service=LabelService(label_service_url),
    )

    streamer_adapter = EthAlertAdapter(
        engine=engine,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=alert_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        is_pending_mode=pending_mode,
    )

    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=0,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=1,
    )
    streamer.stream()

    RuleSets.close()
