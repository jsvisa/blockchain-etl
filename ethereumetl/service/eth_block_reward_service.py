# this script would:
# 1. parse the block's miner
# 2. parse the uncle block's miner and their corresponding reward
# 3. calculate the block reward

from web3 import Web3

# connet to the server, may use IPCProvider or WebsocketProvider
# w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
# PLEASE GIVE A INFURA PROJECT ID FOR TEST IN [proj_id]
w3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/[proj_id]"))


class EthBlockRewardService(object):
    def __init__(self, w3, block_num):
        self._w3 = w3
        self._block_num = block_num
        self._the_block = self._fetch_block()

    def _fetch_block(self):
        # return the dict with full transactions records
        return self._w3.eth.get_block(int(self._block_num), True)

    def calculate_block_reward(self):
        # static block reward
        # it is changed according to EIP-649 and EIP-1234
        if self._the_block["number"] < 4370000:
            static_block_reward = 5 * 10**18
        elif self._the_block["number"] < 7280000:
            static_block_reward = 3 * 10**18
        else:
            static_block_reward = 2 * 10**18

        # txn fees
        txns = self._the_block["transactions"]
        txn_fees = 0
        for txn in txns:
            receipt = self._w3.eth.get_transaction_receipt(txn["hash"])
            # eip-1559 and legacy
            if txn["type"] == "0x2" or txn["type"] == "0x0":
                gas_used, gas_price = receipt["gasUsed"], receipt["effectiveGasPrice"]
                current_txn_fee = gas_used * gas_price
                txn_fees += current_txn_fee
            else:
                raise Exception(f"Unsupported transaction type: {txn['type']}")

        # uncle inclusion reward
        # uncle inclusion reward is: static block reward / 32
        # ref: https://medium.com/@javierggil/ethereum-reward-explained-8f927a1263c6
        uncle_inclusion = (static_block_reward / 32) * len(self._the_block["uncles"])

        # burnt fees
        try:
            burnt_fees = self._the_block["baseFeePerGas"] * self._the_block["gasUsed"]
        except KeyError:
            # as in the previous blocks, there is no burnt fees
            # we manually set it to 0
            burnt_fees = 0

        block_reward = static_block_reward + txn_fees + uncle_inclusion - burnt_fees
        return block_reward

    def parse_miner(self):
        miner = self._the_block["miner"]
        # returns the lowercased string of miner
        return miner.lower()

    def parse_uncle_info(self):
        # this function is to get uncles reward and their miner
        # reutn a list of tuple, in which the first element is the miner, the second is the reward
        uncle_counts = self._w3.eth.get_uncle_count(self._block_num)

        result = []
        for i in range(uncle_counts):
            uncle_body = self._w3.eth.get_uncle_by_block(self._block_num, i)
            uncle_miner = uncle_body["miner"].lower()
            uncle_reward = (int(uncle_body["number"], 16) + 8 - self._block_num) * 2 / 8
            result.append((uncle_miner, uncle_reward))

        return result


def main():
    # this block number is for test
    ethBlockRewardService = EthBlockRewardService(w3, 8369999)

    print("miner:", ethBlockRewardService.parse_miner())
    print("uncle blocks info:", ethBlockRewardService.parse_uncle_info())
    print("block reward:", ethBlockRewardService.calculate_block_reward())
    print("done")


if __name__ == "__main__":
    main()
