from typing import List, Dict, Optional

from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.extract_erc721_transfers_job import ExtractErc721TransfersJob
from ethereumetl.jobs.extract_cryptopunk_transfer_job import (
    ExtractCryptoPunkTransfersJob,
)
from ethereumetl.jobs.extract_erc1155_transfers_job import ExtractErc1155TransfersJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.providers.auto import new_web3_provider
from ethereumetl.service.eth_token_service import EthTokenService


def extract_token_transfers(
    logs,
    batch_size,
    max_workers,
    chain,
    token_service: Optional[EthTokenService] = None,
) -> List[Dict]:
    exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_TRANSFER])
    job = ExtractTokenTransfersJob(
        logs_iterable=logs,
        batch_size=batch_size,
        max_workers=max_workers,
        item_exporter=exporter,
        chain=chain,
        token_service=token_service,
    )
    job.run()
    return exporter.get_items(EntityType.TOKEN_TRANSFER)


def extract_cryptopunk_transfers(logs, chain) -> List[Dict]:
    exporter = InMemoryItemExporter(item_types=[EntityType.ERC721_TRANSFER])
    job = ExtractCryptoPunkTransfersJob(
        logs_iterable=logs, item_exporter=exporter, chain=chain
    )
    job.run()
    return exporter.get_items(EntityType.ERC721_TRANSFER)


def extract_erc721_transfers(
    logs, batch_size, max_workers, erc20_tokens, chain
) -> List[Dict]:
    exporter = InMemoryItemExporter(item_types=[EntityType.ERC721_TRANSFER])
    job = ExtractErc721TransfersJob(
        logs_iterable=logs,
        batch_size=batch_size,
        max_workers=max_workers,
        item_exporter=exporter,
        erc20_tokens=erc20_tokens,
        chain=chain,
    )
    job.run()
    return exporter.get_items(EntityType.ERC721_TRANSFER)


def extract_erc1155_transfers(logs, batch_size, max_workers) -> List[Dict]:
    exporter = InMemoryItemExporter(item_types=[EntityType.ERC1155_TRANSFER])
    job = ExtractErc1155TransfersJob(
        logs_iterable=logs,
        batch_size=batch_size,
        max_workers=max_workers,
        item_exporter=exporter,
    )
    job.run()
    return exporter.get_items(EntityType.ERC1155_TRANSFER)


def extract_contracts(traces, batch_size, max_workers) -> List[Dict]:
    exporter = InMemoryItemExporter(item_types=[EntityType.CONTRACT])
    job = ExtractContractsJob(
        traces_iterable=traces,
        batch_size=batch_size,
        max_workers=max_workers,
        item_exporter=exporter,
    )
    job.run()
    return exporter.get_items(EntityType.CONTRACT)


def extract_tokens(contracts, batch_web3_provider, max_workers) -> List[Dict]:
    exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN])
    job = ExtractTokensJob(
        contracts_iterable=contracts,
        web3=ThreadLocalProxy(lambda: new_web3_provider(batch_web3_provider)),
        max_workers=max_workers,
        item_exporter=exporter,
    )
    job.run()
    return exporter.get_items(EntityType.TOKEN)
