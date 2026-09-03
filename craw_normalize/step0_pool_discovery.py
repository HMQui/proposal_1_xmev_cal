import json
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

ERC20_ABI = """
[
  {
    "constant":true,
    "inputs":[],
    "name":"decimals",
    "outputs":[{"name":"","type":"uint8"}],
    "stateMutability":"view",
    "type":"function"
  }
]
"""

BRIDGES = [
    # 1. CCTP
    # Token: USDC
    # burn_mint    
    # --- Ethereum ---
    {
        "bridge_id": "cctp_usdc_eth_arb",
        "src_chain": "ethereum",
        "dst_chain": "arbitrum",
        "token_src_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "token_dst_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "bridge_family": "cctp",
        "canonical_relation": "burn_mint",
        "bridge_contract": "0xBd3fa81B58Ba92a82136038B25aDec7066af3155",
        "verification_source": "circle_official_docs",
        "proxy_validity_interval": "forever",
        "notes": "TokenMessenger on Ethereum"
    },
    {
        "bridge_id": "cctp_usdc_eth_base",
        "src_chain": "ethereum",
        "dst_chain": "base",
        "token_src_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "token_dst_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "bridge_family": "cctp",
        "canonical_relation": "burn_mint",
        "bridge_contract": "0xBd3fa81B58Ba92a82136038B25aDec7066af3155",
        "verification_source": "circle_official_docs",
        "proxy_validity_interval": "forever",
        "notes": "TokenMessenger on Ethereum"
    },

    # --- Arbitrum ---
    {
        "bridge_id": "cctp_usdc_arb_eth",
        "src_chain": "arbitrum",
        "dst_chain": "ethereum",
        "token_src_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "token_dst_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "bridge_family": "cctp",
        "canonical_relation": "burn_mint",
        "bridge_contract": "0x19330d10D9Cc8751218eaf51E8885D058642E08A",
        "verification_source": "circle_official_docs",
        "proxy_validity_interval": "forever",
        "notes": "TokenMessenger on Arbitrum"
    },
    {
        "bridge_id": "cctp_usdc_arb_base",
        "src_chain": "arbitrum",
        "dst_chain": "base",
        "token_src_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "token_dst_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "bridge_family": "cctp",
        "canonical_relation": "burn_mint",
        "bridge_contract": "0x19330d10D9Cc8751218eaf51E8885D058642E08A",
        "verification_source": "circle_official_docs",
        "proxy_validity_interval": "forever",
        "notes": "TokenMessenger on Arbitrum"
    },

    # --- Base ---
    {
        "bridge_id": "cctp_usdc_base_eth",
        "src_chain": "base",
        "dst_chain": "ethereum",
        "token_src_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_dst_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "bridge_family": "cctp",
        "canonical_relation": "burn_mint",
        "bridge_contract": "0x1682Ae6375C4E4A97e4B583BC394c861A46D8962",
        "verification_source": "circle_official_docs",
        "proxy_validity_interval": "forever",
        "notes": "TokenMessenger on Base"
    },
    {
        "bridge_id": "cctp_usdc_base_arb",
        "src_chain": "base",
        "dst_chain": "arbitrum",
        "token_src_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_dst_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "bridge_family": "cctp",
        "canonical_relation": "burn_mint",
        "bridge_contract": "0x1682Ae6375C4E4A97e4B583BC394c861A46D8962",
        "verification_source": "circle_official_docs",
        "proxy_validity_interval": "forever",
        "notes": "TokenMessenger on Base"
    },
    # 2. Across V3
    # Token: USDC, WETH
    # intent_based / lock_unlock    
    # --- Ethereum ---
    {
        "bridge_id": "across_usdc_eth_arb",
        "src_chain": "ethereum",
        "dst_chain": "arbitrum",
        "token_src_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "token_dst_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Ethereum"
    },
    {
        "bridge_id": "across_weth_eth_arb",
        "src_chain": "ethereum",
        "dst_chain": "arbitrum",
        "token_src_contract": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "token_dst_contract": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Ethereum"
    },
    {
        "bridge_id": "across_usdc_eth_base",
        "src_chain": "ethereum",
        "dst_chain": "base",
        "token_src_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "token_dst_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Ethereum"
    },
    {
        "bridge_id": "across_weth_eth_base",
        "src_chain": "ethereum",
        "dst_chain": "base",
        "token_src_contract": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "token_dst_contract": "0x4200000000000000000000000000000000000006",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Ethereum"
    },

    # --- Arbitrum ---
    {
        "bridge_id": "across_usdc_arb_eth",
        "src_chain": "arbitrum",
        "dst_chain": "ethereum",
        "token_src_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "token_dst_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Arbitrum"
    },
    {
        "bridge_id": "across_weth_arb_eth",
        "src_chain": "arbitrum",
        "dst_chain": "ethereum",
        "token_src_contract": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "token_dst_contract": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Arbitrum"
    },
    {
        "bridge_id": "across_usdc_arb_base",
        "src_chain": "arbitrum",
        "dst_chain": "base",
        "token_src_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "token_dst_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Arbitrum"
    },
    {
        "bridge_id": "across_weth_arb_base",
        "src_chain": "arbitrum",
        "dst_chain": "base",
        "token_src_contract": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "token_dst_contract": "0x4200000000000000000000000000000000000006",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Arbitrum"
    },

    # --- Base ---
    {
        "bridge_id": "across_usdc_base_eth",
        "src_chain": "base",
        "dst_chain": "ethereum",
        "token_src_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_dst_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Base"
    },
    {
        "bridge_id": "across_weth_base_eth",
        "src_chain": "base",
        "dst_chain": "ethereum",
        "token_src_contract": "0x4200000000000000000000000000000000000006",
        "token_dst_contract": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Base"
    },
    {
        "bridge_id": "across_usdc_base_arb",
        "src_chain": "base",
        "dst_chain": "arbitrum",
        "token_src_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_dst_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Base"
    },
    {
        "bridge_id": "across_weth_base_arb",
        "src_chain": "base",
        "dst_chain": "arbitrum",
        "token_src_contract": "0x4200000000000000000000000000000000000006",
        "token_dst_contract": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "bridge_family": "across",
        "canonical_relation": "intent_based",
        "bridge_contract": "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64",
        "verification_source": "across_v3_docs",
        "proxy_validity_interval": "forever",
        "notes": "SpokePool on Base"
    },
    # 3. Stargate 
    # Token: USDC
    # liquidity_network    
    # --- Ethereum ---
    {
        "bridge_id": "stargate_usdc_eth_arb",
        "src_chain": "ethereum",
        "dst_chain": "arbitrum",
        "token_src_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "token_dst_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "bridge_family": "stargate",
        "canonical_relation": "liquidity_network",
        "bridge_contract": "0x8731d54E9D02c286767d56ac03e8037C07e01e98",
        "verification_source": "stargate_docs",
        "proxy_validity_interval": "forever",
        "notes": "Router on Ethereum"
    },
    {
        "bridge_id": "stargate_usdc_eth_base",
        "src_chain": "ethereum",
        "dst_chain": "base",
        "token_src_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "token_dst_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "bridge_family": "stargate",
        "canonical_relation": "liquidity_network",
        "bridge_contract": "0x8731d54E9D02c286767d56ac03e8037C07e01e98",
        "verification_source": "stargate_docs",
        "proxy_validity_interval": "forever",
        "notes": "Router on Ethereum"
    },

    # --- Arbitrum ---
    {
        "bridge_id": "stargate_usdc_arb_eth",
        "src_chain": "arbitrum",
        "dst_chain": "ethereum",
        "token_src_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "token_dst_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "bridge_family": "stargate",
        "canonical_relation": "liquidity_network",
        "bridge_contract": "0x53Bf833A5d6c4ddA888F69c22C88C9f356a41614",
        "verification_source": "stargate_docs",
        "proxy_validity_interval": "forever",
        "notes": "Router on Arbitrum"
    },
    {
        "bridge_id": "stargate_usdc_arb_base",
        "src_chain": "arbitrum",
        "dst_chain": "base",
        "token_src_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "token_dst_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "bridge_family": "stargate",
        "canonical_relation": "liquidity_network",
        "bridge_contract": "0x53Bf833A5d6c4ddA888F69c22C88C9f356a41614",
        "verification_source": "stargate_docs",
        "proxy_validity_interval": "forever",
        "notes": "Router on Arbitrum"
    },

    # --- Base ---
    {
        "bridge_id": "stargate_usdc_base_eth",
        "src_chain": "base",
        "dst_chain": "ethereum",
        "token_src_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_dst_contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "bridge_family": "stargate",
        "canonical_relation": "liquidity_network",
        "bridge_contract": "0x45f1A95A4D3f3836523F5c83673c797f4d4d263B",
        "verification_source": "stargate_docs",
        "proxy_validity_interval": "forever",
        "notes": "Router on Base"
    },
    {
        "bridge_id": "stargate_usdc_base_arb",
        "src_chain": "base",
        "dst_chain": "arbitrum",
        "token_src_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "token_dst_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "bridge_family": "stargate",
        "canonical_relation": "liquidity_network",
        "bridge_contract": "0x45f1A95A4D3f3836523F5c83673c797f4d4d263B",
        "verification_source": "stargate_docs",
        "proxy_validity_interval": "forever",
        "notes": "Router on Base"
    }
]

CHAINS = {
    "ethereum": {
        "chain_id": 1,
        "rpc": os.getenv("ETH_RPC_URL"),
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",

        "tokens": {
            "USDC": {
                "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "wrapper_canonical_relation": "canonical_fiat_backed",
                "verification_source": "circle_official_registry"
            },
            "USDT": {
                "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "wrapper_canonical_relation": "canonical_fiat_backed",
                "verification_source": "tether_official_registry"
            },
            "DAI": {
                "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "wrapper_canonical_relation": "canonical_crypto_backed",
                "verification_source": "makerdao_registry"
            },
            "WETH": {
                "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "wrapper_canonical_relation": "native_wrapper",
                "verification_source": "weth9_contract_code"
            }
        }
    },

    "arbitrum": {
        "chain_id": 42161,
        "rpc": os.getenv("ARB_RPC_URL"),
        "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",

        "tokens": {
            "USDC": {
                "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wrapper_canonical_relation": "canonical_fiat_backed_bridged",
                "verification_source": "arbitrum_bridge_registry"
            },
            "USDT": {
                "address": "0xFd086bC7CD5C481DCC9C85ebe478A1C0b69FCbb9",
                "wrapper_canonical_relation": "canonical_fiat_backed_bridged",
                "verification_source": "arbitrum_bridge_registry"
            },
            "DAI": {
                "address": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
                "wrapper_canonical_relation": "canonical_crypto_backed_bridged",
                "verification_source": "arbitrum_bridge_registry"
            },
            "WETH": {
                "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "wrapper_canonical_relation": "native_wrapper_bridged",
                "verification_source": "arbitrum_bridge_registry"
            }
        }
    },

    "base": {
        "chain_id": 8453,
        "rpc": os.getenv("BASE_RPC_URL"),
        "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",

        "tokens": {
            "USDC": {
                "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "wrapper_canonical_relation": "canonical_fiat_backed_bridged",
                "verification_source": "base_bridge_registry"
            },
            "USDT": {
                "address": "0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2",
                "wrapper_canonical_relation": "canonical_fiat_backed_bridged",
                "verification_source": "base_bridge_registry"
            },
            "DAI": {
                "address": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
                "wrapper_canonical_relation": "canonical_crypto_backed_bridged",
                "verification_source": "base_bridge_registry"
            },
            "WETH": {
                "address": "0x4200000000000000000000000000000000000006",
                "wrapper_canonical_relation": "native_wrapper_bridged",
                "verification_source": "base_bridge_registry"
            }
        }
    }
}

TARGET_PAIRS = [
    ("USDC", "WETH"),
    ("USDT", "WETH"),
    ("DAI", "WETH")
]

FEE_TIERS = [
    100,
    500,
    3000,
    10000
]

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