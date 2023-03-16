ERROR_MAPPER = {
    "out of gas": "Out of gas",
    "execution reverted": "Reverted",
    "invalid jump destination": "Bad jump destination",
}


def geth_error_to_parity(error: str) -> str:
    if error.startswith("invalid opcode: opcode "):
        return "Bad instruction"
    else:
        return ERROR_MAPPER.get(error, error)
