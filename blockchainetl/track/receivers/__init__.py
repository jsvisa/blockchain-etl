import pandas as pd


class BaseReceiver(object):
    def __init__(self, **kwargs):
        kwargs = kwargs
        pass

    def open(self):
        pass

    def post(self, chain: str, result: pd.DataFrame):
        chain, result = chain, result
        raise NotImplementedError

    def close(self):
        pass
