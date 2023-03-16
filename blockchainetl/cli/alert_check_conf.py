import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor

import click

from blockchainetl.alert import rule_udf
from blockchainetl.alert.rule_set import RuleSets
from blockchainetl.cli.utils import evm_chain_options
from blockchainetl.alert.receivers.print_receiver import PrintReceiver


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@evm_chain_options
@click.option(
    "--rule-id",
    type=str,
    show_default=True,
    help="Run with this rule id, used to debug the rule expression",
)
@click.option(
    "--rule-file",
    type=click.Path(exists=True, readable=True, file_okay=True),
    default="etc/rules.yaml",
    show_default=True,
    help="The rule file in YAML",
)
@click.option(
    "--rule-variable-dir",
    type=click.Path(exists=True, readable=True, dir_okay=True),
    default="etc/variables",
    show_default=True,
    help="The rule variable directory",
)
@click.option(
    "-w",
    "--max-workers",
    default=5,
    show_default=True,
    type=int,
    help="The number of workers",
)
@click.option(
    "-Q",
    "--quiet-mode",
    is_flag=True,
    default=False,
    show_default=True,
    help="In quiet mode, the print results will flush into /dev/null",
)
def alert_check_conf(
    chain,
    rule_id,
    rule_file,
    rule_variable_dir,
    max_workers,
    quiet_mode,
):
    from blockchainetl.alert.full_items import FULL_ITEMS

    # rule udf hack.
    rule_udf.ALL["label_of"] = rule_udf.label_of(None, chain, "addr_labels")
    rule_udf.ALL["internal_label_of"] = rule_udf.internal_label_of(None, None)

    # load rulesets from file
    RuleSets.load(rule_file, rule_variable_dir)
    RuleSets.open()
    logging.info(bcolors.OKGREEN + "Rule loading successfully." + bcolors.ENDC)

    ruleset = RuleSets()[chain]
    receivers = RuleSets.receivers
    receiver_keys = set(receivers.keys())
    for rule in ruleset.rules.values():
        rule_extra_receivers = rule.receivers - receiver_keys
        if len(rule_extra_receivers) > 0:
            logging.error(
                bcolors.FAIL
                + "Rule: {rule.id} got unknown receivers: {rule_extra_receivers}"
                + bcolors.ENDC
            )
    logging.info(bcolors.OKGREEN + "Rule receivers check successfully." + bcolors.ENDC)

    if rule_id is not None:
        rule_results = [ruleset.execute_rule(rule_id, FULL_ITEMS)]
    else:
        rule_results = ruleset.execute(ThreadPoolExecutor, FULL_ITEMS, max_workers)

    printer = PrintReceiver(file=open(os.devnull, "w") if quiet_mode else sys.stdout)
    for rid, result in rule_results:
        if isinstance(result, ValueError):
            logging.error(
                bcolors.FAIL + f"failed to execute {rid} {result}" + bcolors.ENDC
            )

        if len(result) == 0:
            continue

        rule = ruleset[rid]
        printer.post(rule, result)

    logging.info(
        bcolors.OKGREEN + "Rule rules.where check successfully." + bcolors.ENDC
    )

    single_items = {scope: FULL_ITEMS[scope][0] for scope in FULL_ITEMS}
    rule_errors = {}
    for rid, rule in ruleset.rules.items():
        if rid is not None and rid != rule_id:
            continue
        try:
            printer.post(rule, [single_items[rule.scope]])
        except Exception as e:
            rule_errors[rid] = e

    if len(rule_errors) > 0:
        for rule_id, error in rule_errors.items():
            logging.error(bcolors.FAIL + f"Rule: {rule_id} error: {error}")

    else:
        logging.info(
            bcolors.OKGREEN
            + "Rule rules.{labels,output} check successfully."
            + bcolors.ENDC
        )
