class Chain:
    BITCOIN = "bitcoin"
    BITCOIN_CASH = "bitcoin_cash"
    BITCOIN_GOLD = "bitcoin_gold"
    DOGECOIN = "dogecoin"
    LITECOIN = "litecoin"
    DASH = "dash"
    ZCASH = "zcash"
    MONACOIN = "monacoin"
    MOONBEAM = "moonbeam"
    MOONRIVER = "moonriver"

    ETHEREUM = "ethereum"
    BSC = "bsc"
    HECO = "heco"
    BOR = "bor"
    AVALANCHE = "avalanche"
    ARBITRUM = "arbitrum"  # arbitrum one/nitro
    NOVA = "nova"  # arbitrum nova
    RONIN = "ronin"
    FANTOM = "fantom"
    WHALEFIN = "whalefin"
    OPTIMISTIC = "optimistic"
    XDAI = "xdai"
    CRONOS = "cronos"
    CELO = "celo"
    ETHC = "ethc"
    ETHW = "ethw"
    ETHF = "ethf"
    AURORA = "aurora"
    HARMONY = "harmony"

    SOLANA = "solana"

    TRON = "tron"

    ALL_BITCOIN_FORKS = [
        BITCOIN,
        BITCOIN_CASH,
        BITCOIN_GOLD,
        DOGECOIN,
        LITECOIN,
        DASH,
        ZCASH,
        MONACOIN,
    ]

    ALL_ETHEREUM_FORKS = [
        ETHEREUM,
        BSC,
        HECO,
        BOR,
        AVALANCHE,
        ARBITRUM,
        NOVA,
        RONIN,
        FANTOM,
        WHALEFIN,
        OPTIMISTIC,
        XDAI,
        CRONOS,
        CELO,
        ETHC,
        ETHW,
        ETHF,
        AURORA,
        HARMONY,
        TRON,
    ]

    # Those chains we have already supported ETL via GreenPlum
    ALL_FOR_ETL = ALL_BITCOIN_FORKS + ALL_ETHEREUM_FORKS + [SOLANA]

    # Old API doesn't support verbosity for getblock which
    # doesn't allow querying all transactions in a block in 1 go.
    HAVE_OLD_API = [BITCOIN_CASH, DOGECOIN, DASH, MONACOIN]

    CHAIN_SYMBOLS = {
        BITCOIN: "BTC",
        ETHEREUM: "ETH",
        BSC: "BNB",
        AVALANCHE: "AVAX",
        HECO: "HT",
        ARBITRUM: "ETH",
        NOVA: "ETH",
        FANTOM: "FTM",
        RONIN: "RON",
        OPTIMISTIC: "ETH",
        XDAI: "XDAI",
        CRONOS: "CRO",
        BOR: "MATIC",
        SOLANA: "SOL",
        CELO: "CELO",
        ETHC: "ETC",
        ETHW: "ETHW",
        ETHF: "ETHF",
        AURORA: "ETH",
        HARMONY: "ONE",
        TRON: "TRX",
    }

    @staticmethod
    def symbol(chain: str) -> str:
        return Chain.CHAIN_SYMBOLS.get(chain, "ETH")
