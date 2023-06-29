from web3 import Web3
from typing import Union, Dict
from .eth_token_service import EthTokenService
from eth_utils.address import to_checksum_address


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
        owner = to_checksum_address(owner)
        return contract.caller(block_identifier=block).balanceOf(owner)

    def eth_erc20_allowance(
        self,
        owner: str,
        spender: str,
        token: str,
        block: Union[int, str] = "latest",
    ) -> int:
        contract = self.token_contract(token)
        owner = to_checksum_address(owner)
        spender = to_checksum_address(spender)
        return contract.caller(block_identifier=block).allowance(owner, spender)

    @classmethod
    def all_udfs(cls, web3: Web3) -> Dict:
        service = cls(web3)
        return {
            "eth_erc20_balanceOf": service.eth_erc20_balanceOf,
            "eth_erc20_allowance": service.eth_erc20_allowance,
        }
