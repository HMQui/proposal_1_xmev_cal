import json
import os

from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

# ---------------------------------------------------------------------------
# Load protocol config from manifest
# ---------------------------------------------------------------------------
_MANIFEST_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "artifact", "manifests", "protocol_config_8w.json"
)

with open(_MANIFEST_PATH, encoding="utf-8") as _f:
    _CFG = json.load(_f)

# ABIs (stored as parsed lists in the manifest, Web3 accepts list or JSON str)
FACTORY_ABI = _CFG["factory_abi"]
ERC20_ABI   = _CFG["erc20_abi"]

BRIDGES     = _CFG["bridges"]
FEE_TIERS   = _CFG["fee_tiers"]

# TARGET_PAIRS: manifest stores lists, convert back to tuples for compatibility
TARGET_PAIRS = [tuple(p) for p in _CFG["target_pairs"]]

# CHAINS: inject live RPC URLs from environment variables
CHAINS = {}
for _chain_name, _chain_data in _CFG["chains"].items():
    _entry = dict(_chain_data)
    _entry["rpc"] = os.getenv(_entry.pop("rpc_env"))
    CHAINS[_chain_name] = _entry

def fetch_decimals(w3, token_address):
    contract = w3.eth.contract(
        address=w3.to_checksum_address(token_address),
        abi=ERC20_ABI
    )

    return contract.functions.decimals().call()


def discover_pool(factory, token_a, token_b, fee):
    return factory.functions.getPool(
        token_a,
        token_b,
        fee
    ).call()


def build_token_registry():
    token_map = {}

    for chain_name, chain_cfg in CHAINS.items():

        w3 = Web3(
            Web3.HTTPProvider(chain_cfg["rpc"])
        )

        if not w3.is_connected():
            raise RuntimeError(
                f"Cannot connect to {chain_name}"
            )

        for symbol, meta in chain_cfg["tokens"].items():

            token_id = (
                f"{chain_cfg['chain_id']}:"
                f"{meta['address'].lower()}"
            )

            token_map[token_id] = {
                "token_id": token_id,
                "asset_class": symbol,
                "chain": chain_name,
                "chain_id": chain_cfg["chain_id"],
                "contract": meta["address"],
                "decimals": fetch_decimals(
                    w3,
                    meta["address"]
                ),
                "wrapper_canonical_relation":
                    meta["wrapper_canonical_relation"],
                "proxy_validity_interval": "forever",
                "verification_source":
                    meta["verification_source"],
                "fee_on_transfer": False,
                "rebasing": False
            }

    return token_map


# Discover all existing pools across fee tiers.
def discover_all_pools():
    discovered_pools = []

    print()
    print("Pool selection rule: discover_all_existing_pools")
    print()

    for chain_name, chain_cfg in CHAINS.items():

        w3 = Web3(
            Web3.HTTPProvider(chain_cfg["rpc"])
        )

        if not w3.is_connected():
            raise RuntimeError(
                f"Cannot connect to {chain_name}"
            )

        factory = w3.eth.contract(
            address=w3.to_checksum_address(
                chain_cfg["factory"]
            ),
            abi=FACTORY_ABI
        )

        print(f"Scanning {chain_name}")

        for token_a_symbol, token_b_symbol in TARGET_PAIRS:

            token_a_addr = w3.to_checksum_address(
                chain_cfg["tokens"][token_a_symbol]["address"]
            )

            token_b_addr = w3.to_checksum_address(
                chain_cfg["tokens"][token_b_symbol]["address"]
            )

            found_pool = False

            for fee in FEE_TIERS:

                pool_address = discover_pool(
                    factory,
                    token_a_addr,
                    token_b_addr,
                    fee
                )

                if int(pool_address, 16) == 0:
                    continue

                found_pool = True

                print(
                    f"  FOUND "
                    f"{token_a_symbol}/{token_b_symbol} "
                    f"fee={fee} "
                    f"pool={pool_address}"
                )

                discovered_pools.append({
                    "chain": chain_name,
                    "chain_id": chain_cfg["chain_id"],
                    "dex_family": "uniswap_v3",
                    "selection_rule": "discovered_existing_pool",
                    "pool_address": pool_address,
                    "token0_contract": min(
                        token_a_addr,
                        token_b_addr,
                        key=lambda x: int(x, 16)
                    ),
                    "token1_contract": max(
                        token_a_addr,
                        token_b_addr,
                        key=lambda x: int(x, 16)
                    ),
                    "fee_tier": fee
                })

            if not found_pool:
                raise RuntimeError(
                    f"No pool found for "
                    f"{chain_name} "
                    f"{token_a_symbol}/{token_b_symbol}"
                )

    return discovered_pools


def main():

    token_map = build_token_registry()

    pools = discover_all_pools()

    output = {
        "registry_version": 1,

        "selection_rule":
            "all_discovered_pools",

        "chains": [
            "ethereum",
            "arbitrum",
            "base"
        ],

        "asset_classes": [
            "USDC",
            "USDT",
            "DAI",
            "WETH"
        ],

        "pools": pools,

        "token_map": token_map,

        "bridges": BRIDGES
    }

    print()
    print("=" * 80)
    print("FINAL REGISTRY")
    print("=" * 80)

    output_filename = "discovered_pools.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4)
    print(f"\nSuccessfully saved registry to {output_filename}")


if __name__ == "__main__":
    main()