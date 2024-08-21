import csv
import io
import psycopg2 as psycopg
from typing import Iterable, List, Dict
from blockchainetl.streaming.postgres_utils import cursor_copy_from_stream
from .converters.composite_item_converter import CompositeItemConverter
from ._utils import group_by_item_type


def list_of_dicts_to_csv(items: List[Dict]) -> str:
    # Use StringIO to simulate a file in memory
    output = io.StringIO()
    fillin_items_to_stream(items, output)

    # Get the CSV string from the StringIO object
    csv_string = output.getvalue()

    # Close the StringIO object
    output.close()

    return csv_string


def fillin_items_to_stream(items: Iterable[Dict], output: io.StringIO):
    # To reset the buffer position to the start so it can be read from the beginning
    output.seek(0)
    # Get the fieldnames from the keys of the first dictionary
    iterator = iter(items)
    try:
        firstrow = next(iterator)
    except StopIteration:
        raise ValueError("The list cannot be empty")

    fieldnames = firstrow.keys()
    # Create a csv writer object
    writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter="^")

    # Write the header
    writer.writeheader()
    writer.writerow(firstrow)

    # Write the data
    for row in iterator:
        writer.writerow(row)

    # To reset the buffer position to the start so it can be read from the beginning
    output.seek(0)


def items2stream(items: Iterable[Dict]) -> io.StringIO:
    output = io.StringIO()
    fillin_items_to_stream(items, output)
    return output


class PsycopgItemExporter:
    def __init__(
        self,
        connection_url,
        dbschema,
        item_type_to_table_mapping,
        converters=(),
        print_sql=True,
    ):
        self.connection_url = connection_url
        self.dbschema = dbschema
        self.item_type_to_table_mapping = item_type_to_table_mapping
        self.converter = CompositeItemConverter(converters)
        self.print_sql = print_sql

    def open(self):
        self.conn = psycopg.connect(self.connection_url)

    def export_items(self, items):
        items_grouped_by_type = group_by_item_type(items)

        rows = 0
        with self.conn.cursor() as cursor:
            for item_type, table in self.item_type_to_table_mapping.items():
                item_group = items_grouped_by_type.get(item_type)
                if item_group is None:
                    continue

                tbl = "{}.{}".format(self.dbschema, table)
                # don't reuse the stream, else some wired data occupied issue will happen
                stream = items2stream(self.convert_items(item_group))

                rows += cursor_copy_from_stream(
                    self.conn, cursor, tbl, stream, delimiter="^"
                )
                stream.close()
        return rows

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        self.conn.close()
