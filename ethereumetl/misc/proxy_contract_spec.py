import re
from typing import Tuple, Optional
from web3 import Web3
from web3.exceptions import ContractLogicError
from eth_utils.crypto import keccak
from eth_utils.address import to_checksum_address

ZERO_ADDR = "0x" + "0" * 40


def proxy_is_contract(web3: Web3, address: str) -> bool:
    if address == ZERO_ADDR:
        return False

    try:
        checksum_address = to_checksum_address(address)
    except ValueError:
        return False

    proxy_code = web3.eth.get_code(checksum_address).hex()
    return proxy_code != "0x"


class ProxyContractSpec:
    EIP1967_PROXY = "eip1967.proxy.implementation"
    OS_PROXY = "org.zeppelinos.proxy.implementation"
    EIP897_PROXY = "implementation()"
    EIP1167_PROXY = "eip1167.minimal.proxy"

    PROXY_REGEXS = {
        EIP1167_PROXY: "^0x363d3d373d3d3d363d73[A-Za-z0-9]{40}5af43d82803e903d91602b57fd5bf3$"
    }

    PROXY_FUNCS = {
        EIP897_PROXY: keccak(text=EIP897_PROXY).hex()[:10],
    }

    PROXY_SLOTS = {
        EIP1967_PROXY: hex(int(keccak(text=EIP1967_PROXY).hex(), 16) - 1),
        OS_PROXY: hex(int(keccak(text=OS_PROXY).hex(), 16)),
    }

    @classmethod
    def get_proxy_contract(cls, web3: Web3, contract: str) -> Optional[Tuple[str, str]]:
        contract = to_checksum_address(contract)

        # 1. check the contract is matched proxy regex in static byte code. if not, go to step 2
        for implementation, regex in cls.PROXY_REGEXS.items():
            try:
                code = web3.eth.get_code(contract).hex()
            except ContractLogicError:
                continue
            proxy = None
            if re.match(regex, code):
                proxy = "0x" + code[11 * 2 : 31 * 2].lower()
            if proxy is not None and proxy_is_contract(web3, proxy):
                return implementation, proxy

        # 2. check the contract is matched proxy func in static byte code. if not, go to step 3
        for implementation, func in cls.PROXY_FUNCS.items():
            try:
                d = web3.eth.call({"gas": 50000, "to": contract, "data": func}).hex()
            except ContractLogicError:
                continue
            proxy = "0x" + d[-40:].lower()
            if proxy_is_contract(web3, proxy):
                return implementation, proxy

        # 3. check the contract is matched proxy slot in storage. if not, return None
        for implementation, slot in cls.PROXY_SLOTS.items():
            try:
                storage = web3.eth.get_storage_at(contract, slot).hex()
            except ContractLogicError:
                continue

            proxy = "0x" + storage[-40:].lower()
            if proxy_is_contract(web3, proxy):
                return implementation, proxy
        return None
