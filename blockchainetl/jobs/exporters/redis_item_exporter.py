import logging
import concurrent.futures

import redis

from blockchainetl.utils import dynamic_batch_iterator
from ._utils import group_by_item_type


class RedisItemExporter:
    def __init__(
        self,
        chain,
        red_url,
        entity_types,
        lua_script,
        exporter,
        threads=2,
    ):
        self.connection_url = red_url
        self.chain = chain
        self.entity_types = entity_types
        self.threads = threads
        self.exporter = exporter

        self.red = redis.from_url(red_url)
        self.sha = self.red.script_load(lua_script)

    def open(self):
        pass

    def export_items(self, items):
        if len(items) == 0:
            return

        items_grouped_by_type = group_by_item_type(items)

        with concurrent.futures.ThreadPoolExecutor(self.threads) as executor:
            futures = []
            for entity_type in self.entity_types:
                item_group = items_grouped_by_type.get(entity_type)
                if item_group is None:
                    continue

                for chunk in dynamic_batch_iterator(item_group, lambda: 100):
                    f = executor.submit(
                        self.exporter, self.chain, self.red, self.sha, chunk
                    )
                    futures.append(f)
            updated = 0
            for f in concurrent.futures.as_completed(futures):
                exception = f.exception()
                updated += f.result()
                if exception:
                    logging.error(exception)
                    raise Exception(exception)

    def close(self):
        pass
