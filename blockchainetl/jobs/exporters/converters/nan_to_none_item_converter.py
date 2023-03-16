import pandas as pd

from .simple_item_converter import SimpleItemConverter


class NanToNoneItemConverter(SimpleItemConverter):
    def convert_field(self, key, value):
        if pd.isna(value):
            return None
        else:
            return value
