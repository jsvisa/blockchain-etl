import json
from datetime import datetime
from requests import Session
from requests.sessions import HTTPAdapter

from typing import Dict, Any, List


class BtcRPCClient:
    __id = 0

    def __init__(self, request_url):
        self._session = Session()
        self._session.mount(
            "http://", HTTPAdapter(pool_connections=100, pool_maxsize=200)
        )
        self._session.mount(
            "https://", HTTPAdapter(pool_connections=100, pool_maxsize=200)
        )
        self._request_url = request_url

    def getrawtransaction(self, txhash: str) -> Dict[str, Any]:
        return self._request("getrawtransaction", [txhash, True])

    def getblock(self, blockhash: str, verbose=1) -> Dict[str, Any]:
        return self._request("getblock", [blockhash, verbose])

    def _request(self, method: str, params: List) -> Dict[str, Any]:
        self.__class__.__id += 1
        body = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.__class__.__id,
        }
        res = self._session.post(self._request_url, json=body)
        if res.status_code / 100 != 2:
            raise Exception(
                json.dumps(
                    {"code": res.status_code, "message": "Server error (%s)" % res.text}
                )
            )
        return res.json()["result"]


class BtcBlockParser:
    def __init__(self, block):
        self._block = block

    def parse(self):
        block = self._block
        blk_st = block["timestamp"]
        utc = datetime.utcfromtimestamp(blk_st)
        st_day = utc.strftime("%Y-%m-%d")

        blk_num = block["number"]
        blk_hash = block["hash"]
        num_txs = block["transaction_count"]

        msg = (
            blk_st,
            st_day,
            blk_num,
            blk_hash,
            num_txs,
            block["size"],
            block["stripped_size"],
            block["weight"],
            block["version"],
            int(block["nonce"], 16),
            int(block["bits"], 16),
        )
        return msg


class BtcUTXOParser:
    def __init__(self, tx):
        self._tx = tx

    def parse(self):
        tx = self._tx
        blk_st = tx["block_timestamp"]
        utc = datetime.utcfromtimestamp(blk_st)
        st_day = utc.strftime("%Y-%m-%d")
        blk_num = tx["block_number"]
        txhash = tx["hash"]
        txpos = tx["index"]

        vin_cnt = tx["input_count"]
        vout_cnt = tx["output_count"]
        is_coinbase = 1 if tx["is_coinbase"] else 0

        if vin_cnt != len(tx["inputs"]):
            raise Exception(
                "#vin of tx ({}) is malforded({} != {})".format(
                    txhash, vin_cnt, len(tx["inputs"])
                )
            )
        if vout_cnt != len(tx["outputs"]):
            raise Exception(
                "#vout of tx ({}) is malforded({} != {})".format(
                    txhash, vout_cnt, len(tx["outputs"])
                )
            )

        for vin in tx["inputs"]:
            pxhash = vin["spent_transaction_hash"]
            pxvout = vin["spent_output_index"]

            msg = "^".join(
                str(e)
                for e in (
                    blk_st,
                    st_day,
                    blk_num,
                    txhash,
                    txpos,
                    1,
                    is_coinbase,
                    pxhash,
                    vin["index"],
                    vin_cnt,
                    pxvout,
                    vout_cnt,
                    "",
                    0,
                )
            )

            yield msg

        for vout in tx["outputs"]:
            if vout["type"] == "nonstandard":
                address = "nonstandard"
            else:
                # Bitcoin multisign address(prefixed by 3) was first introduced in 2011,
                # see https://github.com/bitcoin/bips/blob/master/bip-0013.mediawiki
                # Before this feature available in mainnet, bitcoin's multi-signed addresses
                # are the result of multiple addresses being put together.
                # so we join those addresses together into one address
                # Hint: also limit the address length <= 1024.
                # If an UTXO has multi addresses,
                # this UTXO can be unlocked only if all addresses are provided.
                address = ";".join(vout["addresses"])
                if len(address) > 1024:
                    address = address[:1021] + "..."
            msg = "^".join(
                str(e)
                for e in (
                    blk_st,
                    st_day,
                    blk_num,
                    txhash,
                    txpos,
                    0,
                    is_coinbase,
                    "",
                    0,
                    vin_cnt,
                    vout["index"],
                    vout_cnt,
                    address,
                    vout["value"],
                )
            )
            yield msg
