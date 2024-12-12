import json
from time import time


class StreamerJsonlSkiper:
    def __init__(self, save_path: str):
        self.file = save_path

    def __call__(self, start_block, end_block):
        with open(self.file, mode="a") as fp:
            data = json.dumps(
                {
                    "timestamp": int(time()),
                    "start_block": start_block,
                    "end_block": end_block,
                }
            )
            fp.write(data)
