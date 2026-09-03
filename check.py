import os
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

FACTORY_ABI = """
[
  {
    "inputs":[
      {"internalType":"address","name":"","type":"address"},
      {"internalType":"address","name":"","type":"address"},
      {"internalType":"uint24","name":"","type":"uint24"}
    ],
    "name":"getPool",
    "outputs":[
      {"internalType":"address","name":"","type":"address"}
    ],
    "stateMutability":"view",
    "type":"function"
  }
]
"""

CHAINS = {
    "ethereum": {
        "rpc": os.getenv("ETH_RPC_URL"),
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "tokens": {
            "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "DAI":  "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        }
    },

    "arbitrum": {
        "rpc": os.getenv("ARB_RPC_URL"),
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "tokens": {
            "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "USDT": "0xFd086bC7CD5C481DCC9C85ebe478A1C0b69FCbb9",
            "DAI":  "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        }
    },

    "base": {
        "rpc": os.getenv("BASE_RPC_URL"),
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "tokens": {
            "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "USDT": "0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2",
            "DAI":  "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
            "WETH": "0x4200000000000000000000000000000000000006",
        }
    }
}

PAIRS = [
    ("USDC", "WETH"),
    ("USDT", "WETH"),
    ("DAI",  "WETH"),
]

FEE_TIERS = [
    100,
    500,
    3000,
    10000,
]


def main():

    for chain_name, chain_cfg in CHAINS.items():

        print()
        print("=" * 80)
        print(chain_name.upper())
        print("=" * 80)

        w3 = Web3(
            Web3.HTTPProvider(chain_cfg["rpc"])
        )

        factory = w3.eth.contract(
            address=w3.to_checksum_address(
                chain_cfg["factory"]
            ),
            abi=FACTORY_ABI
        )

        for token_a, token_b in PAIRS:

            print()
            print(f"{token_a}/{token_b}")

            addr_a = w3.to_checksum_address(
                chain_cfg["tokens"][token_a]
            )

            addr_b = w3.to_checksum_address(
                chain_cfg["tokens"][token_b]
            )

            found = False

            for fee in FEE_TIERS:

                pool = factory.functions.getPool(
                    addr_a,
                    addr_b,
                    fee
                ).call()

                if int(pool, 16) != 0:

                    found = True

                    print(
                        f"  fee={fee:<5} "
                        f"pool={pool}"
                    )

            if not found:
                print("  no pool found")


if __name__ == "__main__":
    main()