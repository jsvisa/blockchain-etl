from typing import List
from blockchainetl.jobs.base_job import BaseJob
from bitcoinetl.mappers.trace_mapper import BtcTraceMapper
from bitcoinetl.mappers.transaction_mapper import BtcTransactionMapper
from bitcoinetl.domain.transaction import BtcTransaction


class ExtractTracesJob(BaseJob):
    def __init__(self, transactions_iterable: List[BtcTransaction], item_exporter):
        self.transactions_iterable = transactions_iterable
        self.item_exporter = item_exporter
        self.trace_mapper = BtcTraceMapper()
        self.transaction_mapper = BtcTransactionMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        for transaction in self.transactions_iterable:
            # FIXME: let's rebuild the Transaction struct first
            tx = self.transaction_mapper.dict_to_transaction(transaction)
            for vin in tx.inputs:
                self.item_exporter.export_item(
                    self.trace_mapper.trace_to_dict(
                        self.trace_mapper.vin_to_trace(vin, tx)
                    )
                )

            for vout in tx.outputs:
                self.item_exporter.export_item(
                    self.trace_mapper.trace_to_dict(
                        self.trace_mapper.vout_to_trace(vout, tx)
                    )
                )

    def _end(self):
        self.item_exporter.close()
