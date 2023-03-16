from typing import Dict
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from blockchainetl.enumeration.chain import Chain

from ethereumetl.service.eth_erc20_service import EthErc20Service
from ethereumetl.providers.auto import get_provider_from_uri

from .rule_output import RuleOutput
from . import rule_udf as ru

# from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc


class RuleCondition(RuleOutput):
    def __init__(self, condition: str, chain: str, provider_uri: str):
        super().__init__(condition)
        self._builtins = ru.ALL

        # mixin chain UDF
        if chain in Chain.ALL_ETHEREUM_FORKS:
            eth_udfs = EthErc20Service.all_udfs(
                ThreadLocalProxy(
                    lambda: get_provider_from_uri(provider_uri, batch=True)
                )
            )
            self._builtins.update(eth_udfs)

        # elif chain in Chain.ALL_BITCOIN_FORKS:
        #     provider = ThreadLocalProxy(lambda: BitcoinRpc(provider_uri))

    def __repr__(self):
        return self._template or "none"

    def execute(self, s: Dict) -> bool:
        condition = self.format(s)
        return eval(condition, self._builtins) is True
