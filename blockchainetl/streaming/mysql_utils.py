from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.dialects.mysql.dml import Insert


def create_insert_statement_for_table(
    table: Table,
    on_conflict_do_update: bool = True,
    upsert_callback=None,
):
    insert_stmt: Insert = insert(table)

    primary_key_fields = [column.name for column in table.columns if column.primary_key]
    if primary_key_fields:
        if on_conflict_do_update:
            if upsert_callback is None:
                upserted_columns = {
                    column.name: insert_stmt.inserted[column.name]
                    for column in table.columns
                    if not column.primary_key
                }
            else:
                upserted_columns = upsert_callback(table, insert_stmt)

            insert_stmt = insert_stmt.on_duplicate_key_update(upserted_columns)
        else:
            insert_stmt = insert_stmt.prefix_with("IGNORE")

    return insert_stmt
