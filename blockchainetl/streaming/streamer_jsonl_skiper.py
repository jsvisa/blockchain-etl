from time import time
import jsonlines as jl


class StreamerJsonlSkiper:
    def __init__(self, save_path: str):
        self.file = save_path

    def __call__(self, start_block, end_block):
        with jl.open(self.file, mode="a", flush=True) as fp:
            fp.write(
                {
                    "timestamp": int(time()),
                    "start_block": start_block,
                    "end_block": end_block,
                }
            )
