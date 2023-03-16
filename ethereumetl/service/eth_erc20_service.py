from web3 import Web3
from typing import Union, Dict
from .eth_token_service import EthTokenService


class EthErc20Service(EthTokenService):
    def __init__(self, provider):
        super().__init__(Web3(provider))

    def eth_erc20_balanceOf(
        self,
        owner: str,
        token: str,
        block: Union[int, str] = "latest",
    ) -> int:
        contract = self.token_contract(token)
        owner = self._web3.toChecksumAddress(owner)
        return contract.caller(block_identifier=block).balanceOf(owner)

    def eth_erc20_allowance(
        self,
        owner: str,
        spender: str,
        token: str,
        block: Union[int, str] = "latest",
    ) -> int:
        contract = self.token_contract(token)
        owner = self._web3.toChecksumAddress(owner)
        spender = self._web3.toChecksumAddress(spender)
        return contract.caller(block_identifier=block).allowance(owner, spender)

    @classmethod
    def all_udfs(cls, web3: Web3) -> Dict:
        service = cls(web3)
        return {
            "eth_erc20_balanceOf": service.eth_erc20_balanceOf,
            "eth_erc20_allowance": service.eth_erc20_allowance,
        }
