from typing import Tuple, Union


class StreamerAdapterStub:
    def open(self):
        pass

    def get_current_block_number(self) -> Union[int, Tuple[int, int]]:
        return (0, 0)

    def export_all(self, start_block, end_block):
        start_block, end_block = start_block, end_block
        pass

    def close(self):
        pass
