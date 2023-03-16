# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import logging
import hashlib

from blockchainetl.enumeration.entity_type import EntityType


class BtcItemIdCalculator:
    def calculate(self, item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get("type")

        if (
            item_type in (EntityType.BLOCK, EntityType.TRANSACTION)
            and item.get("hash", item.get("txhash")) is not None
        ):
            return concat_md5(item_type, item.get("hash", item.get("txhash")))
        elif (
            item_type == EntityType.TRACE
            and item.get("txhash") is not None
            and item.get("vout_idx") is not None
        ):
            return concat_md5(
                item_type,
                item.get("txhash"),
                item.get("vout_idx"),
                item.get("pxhash"),
                item.get("vin_idx"),
            )

        logging.warning("item_id for item {} is None".format(json.dumps(item)))

        return None


ITEM_TYPE_MAP = {
    "block": "b",
    "transaction": "t",
    "trace": "r",
}


def concat_md5(item_type, *elements):
    prefix = ITEM_TYPE_MAP.get(item_type, item_type)
    data = prefix + "_" + "_".join([str(elem) for elem in elements])
    return hashlib.md5(data.encode()).hexdigest()
