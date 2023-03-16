class RuleScope:
    BLOCK = "block"
    TX = "tx"
    TRACE = "trace"
    LOG = "log"
    TOKEN_XFER = "token_xfer"

    @classmethod
    def from_str(cls, s: str):
        s = s.lower()
        if s == "block":
            return cls.BLOCK
        elif s == "tx":
            return cls.TX
        elif s == "trace":
            return cls.TRACE
        elif s == "log":
            return cls.LOG
        elif s == "token_xfer":
            return cls.TOKEN_XFER
        else:
            raise ValueError("unknown scope type")
