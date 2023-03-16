import os
import logging
from time import sleep

import click
from prometheus_client import Gauge, start_http_server

from blockchainetl.streaming.streamer import read_last_synced_block


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-l",
    "--last-synced-block-dir",
    type=click.Path(exists=True, dir_okay=True, writable=True, readable=True),
    required=True,
    show_default=True,
    help="The directory used to store the last synchronized block file",
)
@click.option(
    "--listen-address",
    default="0.0.0.0",
    show_default=True,
    type=str,
    help="The exporter listen address",
)
@click.option(
    "--listen-port",
    default=9112,
    show_default=True,
    type=int,
    help="The exporter listen port",
)
@click.option(
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
def dump_exporter(last_synced_block_dir, listen_address, listen_port, period_seconds):
    """Chain dump exporter"""

    g = Gauge("chain_dump_lag", "Chain block dumping lag", ["chain", "task"])

    def collect(lsp_root, period_seconds):
        while True:
            for chain in os.listdir(lsp_root):
                chain_path = os.path.join(lsp_root, chain)
                if not os.path.isdir(chain_path):
                    continue

                for file in os.listdir(chain_path):
                    lsp_file = os.path.join(lsp_root, chain, file)
                    if not os.path.isfile(lsp_file) or not file.endswith(".txt"):
                        continue

                    # rm tailing .txt
                    task = file[:-4]
                    try:
                        blknum = read_last_synced_block(lsp_file)
                    except Exception:
                        blknum = 0
                    g.labels(chain=chain, task=task).set(blknum)

            sleep(period_seconds)

    logging.info(f"Exporter start listening on http://{listen_address}:{listen_port}")
    start_http_server(listen_port, listen_address)
    collect(last_synced_block_dir, period_seconds)
