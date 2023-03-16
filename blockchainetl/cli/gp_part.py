import click
import psycopg2 as psycopg
import pandas as pd
import logging
from time import time
from datetime import date, datetime, timedelta

from blockchainetl.utils import time_elapsed
from blockchainetl.enumeration.entity_type import EntityType, chain_entity_table
from blockchainetl.misc.psycopg import set_psycopg2_waitable


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gp-url", type=str, required=True, help="The GreenPlum conneciton url")
@click.option("--gp-schema", type=str, required=True, help="The GreenPlum schema")
@click.option(
    "-T",
    "--gp-tables",
    type=str,
    default=",".join(EntityType.ALL_FOR_ETL),
    required=True,
    show_default=True,
    help="The list of partition tables to be created, based on entity-type, split by ','.",
)
@click.option(
    "-s",
    "--start-date",
    default=None,
    required=True,
    show_default=True,
    help="Start datetime(included)",
)
@click.option(
    "-e",
    "--end-date",
    default=datetime.utcnow().date(),
    show_default=True,
    help="End datetime(excluded)",
)
@click.option(
    "--alter",
    type=click.Choice(["add", "split"]),
    default="split",
    help="Alter table partition with 'add parition' or 'split default partition';"
    "'add' new partition only available if the table is empty, else GreenPlum refuse to add;"
    "'split' works when the table already contains some data, and split a new from the DEFAULT PARTITION",  # noqa
)
@click.option(
    "--freq-days",
    default=1,
    show_default=True,
    help="Partition in x days",
)
@click.option(
    "--monthly",
    is_flag=True,
    default=False,
    help="Partition in monthly",
)
@click.option(
    "--dryrun",
    is_flag=True,
    default=False,
    help="Dry run",
)
def gp_part(
    gp_url,
    gp_schema,
    gp_tables,
    start_date,
    end_date,
    alter,
    freq_days,
    monthly,
    dryrun,
):
    """Generate GreenPlum partitions"""
    set_psycopg2_waitable()
    gp_tables = [e for e in gp_tables.strip().split(",") if e != ""]

    gp = psycopg.connect(gp_url)

    def partition_exists(cursor, tbl: str, part: str) -> bool:
        cursor.execute(f"SELECT to_regclass('{tbl}_1_prt_p{part}')")
        (regname,) = cursor.fetchone()
        return regname is not None

    def to_alter_state(st: str, et: str) -> str:
        if alter == "add":
            return (
                f"ALTER TABLE {tbl} ADD PARTITION p{int(st.replace('-', ''))} "
                f"START ('{st}'::date) END ('{et}'::date)"
            )
        return (
            f"ALTER TABLE {tbl} SPLIT DEFAULT PARTITION START ('{st}'::date) END ('{et}'::date) "
            f"INTO (PARTITION p{int(st.replace('-', ''))}, DEFAULT PARTITION)"
        )

    st_dates = list(
        pd.date_range(
            start_date,
            end_date,
            inclusive="left",
            freq="1MS" if monthly else f"{freq_days}d",
        )
    )
    if len(st_dates) == 0:
        raise ValueError(
            "--start-date/--end-date with --freq-days or --monthly can't produce avaliable dates"
        )

    et_dates = st_dates[1:]
    end_day = st_dates[-1] + timedelta(days=32 if monthly else freq_days)
    if monthly:
        end_day = date(end_day.year, end_day.month, 1)
    et_dates.append(end_day)

    assert len(st_dates) == len(et_dates), "start dates don't match to end dates"

    dates = zip(st_dates, et_dates)

    with gp.cursor() as cursor:
        for sday, eday in dates:
            for entity_type in gp_tables:
                tbl = chain_entity_table(gp_schema, entity_type, ignore_unknown=True)
                st, et = sday.strftime("%Y-%m-%d"), eday.strftime("%Y-%m-%d")
                if partition_exists(cursor, tbl, st.replace("-", "")):
                    logging.info(f"tbl: {tbl} partition: {st} exists, skip")
                    continue

                statement = to_alter_state(st, et)

                if dryrun is True:
                    logging.info(f"RUN {statement}")
                    continue

                st = time()
                cursor.execute(statement)
                logging.info(f"{statement} -> (elapsed: {time_elapsed(st)}s)")
                gp.commit()

    gp.close()
