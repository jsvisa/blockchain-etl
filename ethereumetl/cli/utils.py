import os
from typing import Dict, List

import yaml
import pandas as pd
from yamlinclude import YamlIncludeConstructor

from ethereumetl.utils import is_valid_ethereum_address


def validate_and_read_tokens(tokens_file) -> Dict[str, List]:
    with open(tokens_file) as fr:
        YamlIncludeConstructor.add_to_loader_class(
            loader_class=yaml.FullLoader,
            base_dir=os.path.dirname(tokens_file),
        )
        tokens = yaml.load(fr, Loader=yaml.FullLoader)

    # YamlIncludeConstructor will return None if file is empty
    # Let's replace None with empty list
    for kind, items in tokens.items():
        if items is None:
            tokens[kind] = []

    erc20_tokens = tokens.get("erc20_tokens", [])
    erc721_tokens = tokens.get("erc721_tokens", [])
    erc1155_tokens = tokens.get("erc1155_tokens", [])
    full_tokens = erc20_tokens + erc721_tokens + erc1155_tokens
    for token in full_tokens:
        if not is_valid_ethereum_address(token["address"]):
            raise ValueError(f"{token['address']} is not a valid Ethereum address")

    df = pd.DataFrame(full_tokens)
    df_grouped: pd.DataFrame = (
        df.groupby(["address"])["name"]
        .count()
        .reset_index()
        .rename(columns={"name": "count"})  # type: ignore
    )
    dup_df: pd.DataFrame = df_grouped[df_grouped["count"] > 1]
    if dup_df.shape[0] > 0:
        raise ValueError(
            f"duplicated token address in file: {tokens_file} {dup_df['address']}"
        )

    return tokens
