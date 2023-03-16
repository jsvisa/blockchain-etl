def top_holder_exporter_legacy(chain, red, sha, chunk):
    updated = 0
    for item in chunk:
        token_with_id = f"{item['token_address']}-{item.get('token_id', '0')}"
        address = item["address"]

        updated += red.evalsha(
            sha,
            7,
            f"{chain}:token-holder:{token_with_id}:in_value",
            f"{chain}:token-holder:{token_with_id}:ou_value",
            f"{chain}:token-holder:{token_with_id}:in_count",
            f"{chain}:token-holder:{token_with_id}:ou_count",
            f"{chain}:token-holder:{token_with_id}:v_remain",
            f"{chain}:token-holder:{token_with_id}:c_remain",
            f"{chain}:token-holder:{token_with_id}:l_blknum",
            token_with_id,
            address,
            int(item["updated_blknum"]),
            int(item["recv_value"]),
            int(item["send_value"]),
            int(item["recv_count"]),
            int(item["send_count"]),
        )
    return updated


def top_holder_exporter(chain, red, sha, chunk):
    updated = 0
    for item in chunk:
        token_with_id = f"{item['token_address']}-{item.get('token_id', 'null')}"
        address = item["address"]

        updated += red.evalsha(
            sha,
            7,
            f"{chain}:th:{token_with_id}:in_value",
            f"{chain}:th:{token_with_id}:ou_value",
            f"{chain}:th:{token_with_id}:in_count",
            f"{chain}:th:{token_with_id}:ou_count",
            f"{chain}:th:{token_with_id}:v_remain",
            f"{chain}:th:{token_with_id}:c_remain",
            f"{chain}:th:{token_with_id}:l_blknum",
            token_with_id,
            address,
            int(item["updated_blknum"]),
            int(item["recv_value"]),
            int(item["send_value"]),
            int(item["recv_count"]),
            int(item["send_count"]),
        )
    return updated
