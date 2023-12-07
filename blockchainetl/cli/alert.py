import logging

import click
from web3 import HTTPProvider, Web3

from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.alert import rule_udf
from blockchainetl.alert.rule_set import RuleSets
from blockchainetl.cli.utils import (
    extract_cmdline_kwargs,
    global_click_options,
    pick_random_provider_uri,
    str2bool,
)
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType, parse_entity_types
from blockchainetl.jobs.exporters.alert_exporter import AlertExporter
from blockchainetl.service.simple_price_service import SimplePriceService
from blockchainetl.service.price_service import PriceService
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter


# pass kwargs, ref https://stackoverflow.com/a/36522299/2298986
@click.command(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.pass_context
@global_click_options
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
    "--lag",
    default=0,
    show_default=True,
    type=int,
    help="The number of blocks to lag behind the network.",
)
@click.option(
    "-p",
    "--provider-uri",
    default="https://mainnet.infura.io",
    show_default=True,
    type=str,
    envvar="BLOCKCHAIN_ETL_PROVIDER_URI",
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "--price-url",
    type=str,
    default=None,
    help="The price connection url, used in price service",
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
    default=10,
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
    "--pid-file",
    default=None,
    show_default=True,
    type=str,
    help="The pid file",
)
@click.option(
    "--token-cache-path",
    type=click.Path(exists=False, readable=True, dir_okay=True, writable=True),
    show_default=True,
    help="The path to store token's attributes",
)
@click.option(
    "--label-service-url",
    type=str,
    default=None,
    envvar="BLOCKCHAIN_ETL_LABEL_SERVICE_URL",
    help="LabelService URL, used to fetch address labels",
)
def alert(
    ctx,
    chain,
    last_synced_block_file,
    lag,
    provider_uri,
    price_url,
    start_block,
    end_block,
    entity_types,
    rule_id,
    rule_file,
    rule_variable_dir,
    period_seconds,
    batch_size,
    max_workers,
    pid_file,
    token_cache_path,
    label_service_url,
):
    """Alert the live stream with rules"""
    # rule udf hack.
    rule_udf.ALL["label_of"] = rule_udf.label_of(
        label_service_url, chain, "addr_labels"
    )

    entity_types = parse_entity_types(entity_types)

    kwargs = extract_cmdline_kwargs(ctx)
    logging.info(f"Start alert with extra kwargs {kwargs}")

    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using provider: " + provider_uri)

    # load rulesets from file
    RuleSets.load(rule_file, rule_variable_dir)
    RuleSets.open()

    ruleset = RuleSets()[chain]
    receivers = RuleSets.receivers

    token_service = None
    if chain in Chain.ALL_ETHEREUM_FORKS:
        web3 = Web3(HTTPProvider(provider_uri))
        token_service = EthTokenService(web3, cache_path=token_cache_path)

    if price_url is not None:
        price_service = PriceService(price_url)
    else:
        price_service = SimplePriceService()

    alert_exporter = AlertExporter(
        chain,
        ruleset,
        receivers,
        max_workers=max_workers,
        rule_id=rule_id,
        token_service=token_service,
        price_service=price_service,
    )

    if chain in Chain.ALL_ETHEREUM_FORKS:
        streamer_adapter = EthStreamerAdapter(
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            item_exporter=alert_exporter,
            chain=chain,
            batch_size=batch_size,
            max_workers=max_workers,
            entity_types=entity_types,
            is_geth_provider=str2bool(kwargs.get("provider_is_geth")),
            check_transaction_consistency=str2bool(
                kwargs.get("check_transaction_consistency")
            ),
            ignore_receipt_missing_error=str2bool(
                kwargs.get("ignore_receipt_missing_error")
            ),
        )
    elif chain in Chain.ALL_BITCOIN_FORKS:
        streamer_adapter = BtcStreamerAdapter(
            bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
            item_exporter=alert_exporter,
            chain=chain,
            enable_enrich=True,
            batch_size=batch_size,
            max_workers=max_workers,
        )
    else:
        raise NotImplementedError(
            f"--chain({chain}) is not supported in entity types({entity_types})) "
        )

    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=1,
        pid_file=pid_file,
    )
    streamer.stream()

    RuleSets.close()
