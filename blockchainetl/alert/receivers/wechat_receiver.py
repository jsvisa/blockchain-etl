import logging
import requests
from typing import List, Dict

from . import BaseReceiver
from ..rule import Rule


class WechatReceiver(BaseReceiver):

    WECHAT_TITLE_COLORS = {
        "green": "info",
        "gray": "comment",
        "red": "warning",
    }

    def __init__(
        self,
        token: str,
        webhook: str,
        title: str,
        title_color: str = "green",
    ):
        self._token = token
        self._webhook = webhook
        self._title = title
        self._title_color = title_color
        logging.info(f"send notify to {webhook}?key={token}")
        super().__init__()

    def post(
        self,
        rule: Rule,
        result: List[Dict],
    ):
        message = [f"> chain: `{rule.chain}`"]
        for item in result:
            labels = [
                f"> {k.title()}: `{v}`" for k, v in rule.labels.format(item).items()
            ]
            message.extend(labels)
            message.append(rule.output.format(item))
        body = "\n".join(message)

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": f"""
# <font color="{self._color(self._title_color)}">{self._title}</font>

## Rule: `{rule.id}`
## Note: `{rule.description}`

{body}
""",
            },
            "mentioned_list": [],
        }

        res = requests.post(self._push_url(), json=payload)
        if res.status_code // 100 != 2:
            logging.error(
                f"failed to push payload: {payload} to wechat, status: {res.status_code}, text: {res.text}\n"
            )

    def _push_url(self):
        return f"{self._webhook}?key={self._token}"

    def _color(self, color: str):
        return self.WECHAT_TITLE_COLORS.get(color, "black")
