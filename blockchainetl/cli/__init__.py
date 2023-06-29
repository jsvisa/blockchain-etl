import click

# base tasks
from blockchainetl.logging_utils import logging_basic_config
from blockchainetl.signal_utils import configure_signals
from blockchainetl.cli.dump import dump
from blockchainetl.cli.dump2 import dump2
from blockchainetl.cli.reorg import reorg
from blockchainetl.cli.load import load
from blockchainetl.cli.alert import alert
from blockchainetl.cli.alert2 import alert2
from blockchainetl.cli.alert_check_conf import alert_check_conf
from blockchainetl.cli.track import track
from blockchainetl.cli.track2 import track2
from blockchainetl.cli.easy_dump import easy_dump
from blockchainetl.cli.dump_exporter import dump_exporter
from blockchainetl.cli.extract_balance import extract_balance
from blockchainetl.cli.export_balance import export_balance

# extra tasks
from blockchainetl.cli.enrich import enrich

# from blockchainetl.cli.gp_autofix import gp_autofix

from bitcoinetl.cli.utdb import btc_utdb

from ethereumetl.cli.trace import eth_trace
from ethereumetl.cli.export_tokens import export_tokens
from ethereumetl.cli.extract_tokens import extract_tokens
from ethereumetl.cli.extract_contracts import extract_contracts
from ethereumetl.cli.export_token_holders import export_token_holders
from ethereumetl.cli.export_token_transfers import export_token_transfers
from ethereumetl.cli.export_top_holders import export_top_holders

# from ethereumetl.cli.export_nft_tokenids import export_nft_tokenids
# from ethereumetl.cli.export_nft_orderbooks import export_nft_orderbooks
# from ethereumetl.cli.export_uncle_blocks import export_uncle_blocks
from ethereumetl.cli.export_contracts import export_contracts
from ethereumetl.cli.get_block_range_for_date import get_block_range_for_date
from ethereumetl.cli.export_txpool import export_txpool

logging_basic_config()
configure_signals()


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.version_option(version="v3.1.0")
@click.pass_context
def cli(ctx):
    ctx = ctx
    pass


# Chain tasks
cli.add_command(dump, "dump")
cli.add_command(dump2, "dump2")
cli.add_command(reorg, "reorg")
cli.add_command(load, "load")
cli.add_command(enrich, "enrich")
cli.add_command(alert, "alert")
cli.add_command(alert2, "alert2")
cli.add_command(alert_check_conf, "alert-check-conf")
cli.add_command(track, "track")
cli.add_command(track2, "track2")
cli.add_command(easy_dump, "easy-dump")
cli.add_command(dump_exporter, "dump-exporter")
cli.add_command(extract_balance, "extract-balance")
cli.add_command(export_balance, "export-balance")

# GreenPlum tasks
# cli.add_command(gp_autofix, "gp-autofix")

# Bitcoin tasks
cli.add_command(btc_utdb, "btc.utdb")

# Ethereum tasks
cli.add_command(eth_trace, "eth.trace")
cli.add_command(export_tokens, "eth.export-token")
cli.add_command(extract_tokens, "eth.extract-token")
cli.add_command(extract_contracts, "eth.extract-contract")
cli.add_command(export_contracts, "eth.export-contract")
# cli.add_command(extract_token_holders, "eth.extract-token-holder")
cli.add_command(export_token_holders, "eth.export-token-holder")
cli.add_command(export_token_transfers, "eth.export-token-transfer")
cli.add_command(export_top_holders, "eth.export-top-holder")
# cli.add_command(export_nft_tokenids, "eth.export-nft-tokenid")
# cli.add_command(export_nft_orderbooks, "eth.export-nft-orderbook")
# cli.add_command(export_uncle_blocks, "eth.export-uncle-block")
cli.add_command(get_block_range_for_date, "eth.block-range-for-date")
cli.add_command(export_txpool, "eth.export-txpool")
