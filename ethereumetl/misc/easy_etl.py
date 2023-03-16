from typing import Optional, Union, Tuple
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.chain import Chain
from blockchainetl.enumeration.entity_type import EntityType
from blockchainetl.jobs.exporters.file_item_exporter import FileItemExporter
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter


def easy_etl(
    chain: Chain,
    provider_uri: str,
    blknum: Union[int, Tuple[int, int]],
    entity_type: EntityType,
    output: Optional[str] = None,
    batch_size=10,
    max_workers=1,
    df_saver=None,
    output_file: Optional[str] = None,
    is_geth_provider=True,
    retain_precompiled_calls=True,
    check_transaction_consistency=False,
    ignore_receipt_missing_error=False,
):
    item_exporter = FileItemExporter(
        chain, output, output_file=output_file, df_saver=df_saver
    )
    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=item_exporter,
        chain=chain,
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=[entity_type],
        is_geth_provider=is_geth_provider,
        retain_precompiled_calls=retain_precompiled_calls,
        check_transaction_consistency=check_transaction_consistency,
        ignore_receipt_missing_error=ignore_receipt_missing_error,
    )
    if isinstance(blknum, int):
        start_block, end_block = blknum, blknum
    else:
        start_block, end_block = blknum[0], blknum[1]
    streamer_adapter.open()
    streamer_adapter.export_all(start_block, end_block)
    streamer_adapter.close()
