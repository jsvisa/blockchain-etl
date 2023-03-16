import logging
import json
import pandas as pd

from . import BaseReceiver

LOGGER = logging.getLogger(__name__)


class PrintReceiver(BaseReceiver):
    def post(self, chain: str, result: pd.DataFrame):
        data = []
        for item in result:
            data.append(item.to_dict())
        LOGGER.info(f"Receive trackinig from chain: {chain}")
        LOGGER.info(json.dumps(data, indent=2))
