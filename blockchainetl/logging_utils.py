import os
import sys
import logging
import traceback
from .slack_logger import SlackHandler, SlackFormatter


class ShutdownHandler(logging.Handler):
    def emit(self, record):
        stack = traceback.format_exc()
        msg = f"msg: {record}\n stack: {stack}"
        print(msg, file=sys.stderr)
        logging.shutdown()
        sys.exit(1)


def logging_basic_config(filename=None):
    format = "%(asctime)s - %(name)s [%(levelname)s] - %(message)s"
    if filename is not None:
        logging.basicConfig(level=logging.INFO, format=format, filename=filename)
    else:
        logging.basicConfig(level=logging.INFO, format=format, stream=sys.stdout)

    slack_user = os.environ.get(
        "BLOCKCHAIN_ETL_LOGGING_SLACK_USER", "Blockchain-ETL Logger"
    )
    slack_webhook = os.environ.get("BLOCKCHAIN_ETL_LOGGING_SLACK_WEBHOOK")
    slack_channel = os.environ.get("BLOCKCHAIN_ETL_LOGGING_SLACK_CHANNEL")
    add_slack_logger(
        slack_webhook,
        slack_user,
        ":robot_face:",
        slack_channel,
        logging.FATAL,
        SlackFormatter(),
    )
    # add shutdown handler
    logging.getLogger().addHandler(ShutdownHandler(level=logging.FATAL))


def add_slack_logger(
    slack_webhook, slack_user, icon_emoji, slack_channel, log_level, log_formater
):
    # add slack handler if webhook is given
    if slack_webhook is None or slack_webhook == "":
        return

    if log_level <= logging.INFO:
        logging.basicConfig(level=logging.DEBUG)

    sh = SlackHandler(
        username=slack_user,
        icon_emoji=icon_emoji,
        url=slack_webhook,
        channel=slack_channel,
    )
    sh.setLevel(log_level)
    sh.setFormatter(log_formater)
    logging.getLogger().addHandler(sh)
