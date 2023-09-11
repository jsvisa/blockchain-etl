import os
import click
import pandas as pd

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.jobs.trace_transaction_job import TraceTransactionJob
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-p",
    "--provider-uri",
    show_default=True,
    type=str,
    help="The URI of the JSON-RPC's provider.",
)
@click.option(
    "-G",
    "--provider-is-geth",
    show_default=True,
    is_flag=True,
    default=False,
    help="This provider is Geth or not",
)
@click.option(
    "-R",
    "--retain-precompiled-calls",
    show_default=True,
    is_flag=True,
    default=False,
    help="Retain precompiled calls, only available if provider is Geth",
)
@click.option(
    "-x",
    "--txhash",
    type=str,
    show_default=True,
    help="The txhash",
)
@click.option(
    "-B",
    "--block",
    type=int,
    show_default=True,
    help="The block number",
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
    "-o",
    "--output",
    show_default=True,
    help="Save result into this file",
)
def eth_trace(
    provider_uri,
    provider_is_geth,
    retain_precompiled_calls,
    txhash,
    block,
    batch_size,
    max_workers,
    output,
):
    """Extract Geth/Parity's traces"""

    if output is not None:
        output_format = os.path.splitext(output)[1]
        if output_format not in (".json", ".csv"):
            raise click.BadParameter("--output should endswith '.json' or '.csv'")

    exporter = InMemoryItemExporter(
        item_types=[EntityType.TRACE, EntityType.TRANSACTION]
    )

    if txhash is not None:
        job = TraceTransactionJob(
            txhash=txhash,
            provider_uri=provider_uri,
            item_exporter=exporter,
            is_geth_provider=provider_is_geth,
            retain_precompiled_calls=retain_precompiled_calls,
        )
        job.run()

    elif block is not None:
        batch_web3_provider = ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        )

        block_txhashes = {block: dict()}
        if provider_is_geth is True:
            job = ExportBlocksJob(
                start_block=block,
                end_block=block,
                batch_size=batch_size,
                batch_web3_provider=batch_web3_provider,
                max_workers=max_workers,
                item_exporter=exporter,
                export_blocks=False,
                export_transactions=True,
            )
            job.run()
            transactions = exporter.get_items(EntityType.TRANSACTION)
            for tx in transactions:
                block_txhashes[tx["block_number"]][tx["transaction_index"]] = tx["hash"]

        job = ExportTracesJob(
            start_block=block,
            end_block=block,
            batch_size=batch_size,
            batch_web3_provider=batch_web3_provider,
            max_workers=max_workers,
            item_exporter=exporter,
            include_genesis_traces=True,
            include_daofork_traces=True,
            is_geth_provider=provider_is_geth,
            retain_precompiled_calls=retain_precompiled_calls,
            txhash_iterable=block_txhashes,
        )
        job.run()

    else:
        raise click.BadParameter("--block or --txhash should be given")

    traces = exporter.get_items(EntityType.TRACE)
    df = pd.DataFrame(traces)
    df = df.astype({"gas": "Int64", "gas_used": "Int64", "transaction_index": "Int64"})
    if output is not None:
        if output.endswith(".json"):
            df.to_json(output, indent=2, orient="records")
        elif output.endswith(".csv"):
            df.to_csv(output, index=False)
        else:
            raise ValueError("-output is not endswith (.json, .csv)")
