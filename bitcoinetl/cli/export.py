import redis
import click
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.enumeration.entity_type import EntityType

from bitcoinetl.service.btc_exporter import FileItemExporter
from blockchainetl.service.redis_stream_service import RED_NOTIFY_BTC_NEWLY_BLOCK_SCRIPT


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-l",
    "--last-synced-block-file",
    type=str,
    default=".priv/last_synced_block.txt",
    show_default=True,
    help="The file with the last synced block number.",
)
@click.option(
    "--lag",
    type=int,
    default=5,
    show_default=True,
    help="The number of blocks to lag behind the network.",
)
@click.option(
    "-p",
    "--provider-uri",
    type=str,
    show_default=True,
    help="The URI of the remote Bitcoin node.",
)
@click.option(
    "-o",
    "--output",
    type=str,
    default="/etl/btc-data",
    show_default=True,
    help="Output directory",
)
@click.option("-s", "--start-block", type=int, default=None, help="Start block.")
@click.option("-e", "--end-block", type=int, default=None, help="End block.")
@click.option(
    "--tidb-url",
    type=str,
    default="mysql://user:pass@127.0.0.1:4000/blockchain",
    show_default=True,
    help="The TiDB connector.",
)
@click.option(
    "--period-seconds",
    type=int,
    default=10,
    show_default=True,
    help="How many seconds to sleep between syncs.",
)
@click.option(
    "-b",
    "--batch-size",
    type=int,
    default=2,
    show_default=True,
    help="How many blocks to batch in single request.",
)
@click.option(
    "-w",
    "--max-workers",
    type=int,
    default=1,
    show_default=True,
    help="The number of workers.",
)
@click.option(
    "--export-block",
    type=bool,
    default=True,
    show_default=True,
    help="Export block or not.",
)
@click.option(
    "--export-tx",
    type=bool,
    default=True,
    show_default=True,
    help="Export transaction or not.",
)
@click.option(
    "--notify-syncgp",
    type=bool,
    default=True,
    show_default=True,
    help="Notify the newly exported block to GreenPlum or not.",
)
@click.option(
    "--notify-balance",
    type=bool,
    default=True,
    show_default=True,
    help="Notify the newly exported block to Realtime balance or not.",
)
@click.option(
    "--redis-url",
    type=str,
    default="redis://@127.0.0.1:6379/4",
    show_default=True,
    help="The Redis conneciton url used to store notify messages.",
)
@click.option(
    "--notify-syncgp-queue",
    type=str,
    default="btc:export:sync_queue",
    show_default=True,
    help="The Notify syncgp Redis queue",
)
@click.option(
    "--notify-balance-zset",
    type=str,
    default="btc:export:balance_zset",
    show_default=True,
    help="The Notify balance Redis sorted set",
)
@click.option("--pid-file", type=str, default=None, help="The pid file.")
def export(
    last_synced_block_file,
    lag,
    provider_uri,
    output,
    start_block,
    end_block,
    tidb_url,
    period_seconds=10,
    batch_size=2,
    max_workers=5,
    export_block=True,
    export_tx=True,
    notify_syncgp=True,
    notify_balance=True,
    redis_url="redis://@127.0.0.1:6379/4",
    notify_syncgp_queue="btc:export:sync_queue",
    notify_balance_zset="btc:export:balance_zset",
    pid_file=None,
):
    """Streams all data to local file system, and then LOAD into TiDB."""

    def notify(blknum, entity_type, entity_file):
        if notify_syncgp is False and notify_balance is False:
            return

        # bitcoin we only have transaction entity type
        if entity_type != EntityType.TRANSACTION:
            return

        red = redis.from_url(redis_url)

        # don't worry the re script loading phase
        sha = red.script_load(RED_NOTIFY_BTC_NEWLY_BLOCK_SCRIPT)
        red.evalsha(
            sha,
            2,
            notify_syncgp_queue,
            notify_balance_zset,
            blknum,
            entity_file,
            int(notify_syncgp),
            int(notify_balance),
        )

    streamer_adapter = BtcStreamerAdapter(
        bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
        item_exporter=FileItemExporter(
            output,
            tidb_url,
            export_block,
            export_tx,
            do_update=True,
            notify_callback=notify,
        ),
        batch_size=batch_size,
        enable_enrich=False,
        max_workers=max_workers,
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
