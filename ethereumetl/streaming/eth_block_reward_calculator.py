from typing import List, Dict


class EthBlockRewardCalculator:
    def __init__(self, chain):
        self.chain = chain

    def calculate(self, start_block: int, end_block: int) -> List[Dict]:
        return []
