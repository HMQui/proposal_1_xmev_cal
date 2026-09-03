import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3


load_dotenv()


ARTIFACT_DIR = Path("artifact")
MANIFEST_DIR = ARTIFACT_DIR / "manifests"

START_TIME = datetime(2025, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
END_TIME = datetime(2025, 9, 1, 23, 59, 59, tzinfo=timezone.utc)


@dataclass
class BlockMetadata:
    number: int
    hash: str
    timestamp: int


@dataclass
class ChainBlockRange:
    chain_id: int
    start: BlockMetadata
    end: BlockMetadata
    finality_rule: str
    rpc_method: str


def create_web3(rpc_url: str) -> Web3:
    if not rpc_url:
        raise ValueError("RPC URL is not configured.")

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))

    if not w3.is_connected():
        raise ConnectionError("Cannot connect to RPC.")

    return w3


def get_chain_id(w3: Web3) -> int:
    return w3.eth.chain_id


def get_finalized_block_number(w3: Web3) -> int:
    try:
        block = w3.eth.get_block("finalized")
    except Exception as exc:
        raise RuntimeError(
            "RPC does not support the 'finalized' block tag. "
            "The resolver refuses to fall back to 'latest'."
        ) from exc

    return block.number


def get_block_timestamp(w3: Web3, block_number: int) -> int:
    return w3.eth.get_block(block_number).timestamp


def resolve_first_block_at_or_after(
    w3: Web3,
    target_timestamp: int,
    finalized_block: int,
) -> int:
    """Find the first finalized block whose timestamp is >= target."""

    low = 0
    high = finalized_block

    while low < high:
        mid = (low + high) // 2
        timestamp = get_block_timestamp(w3, mid)

        if timestamp < target_timestamp:
            low = mid + 1
        else:
            high = mid

    if get_block_timestamp(w3, low) < target_timestamp:
        raise ValueError("No block found at or after target timestamp.")

    return low


def resolve_last_block_at_or_before(
    w3: Web3,
    target_timestamp: int,
    finalized_block: int,
) -> int:
    """Find the last finalized block whose timestamp is <= target."""

    low = 0
    high = finalized_block

    while low < high:
        mid = (low + high + 1) // 2
        timestamp = get_block_timestamp(w3, mid)

        if timestamp <= target_timestamp:
            low = mid
        else:
            high = mid - 1

    if get_block_timestamp(w3, low) > target_timestamp:
        raise ValueError("No block found at or before target timestamp.")

    return low


def get_block_metadata(w3: Web3, block_number: int) -> BlockMetadata:
    block = w3.eth.get_block(block_number)

    block_hash = block.hash.hex()

    return BlockMetadata(
        number=block.number,
        hash=block_hash,
        timestamp=block.timestamp,
    )


def resolve_chain_range(
    w3: Web3,
    start_timestamp: int,
    end_timestamp: int,
) -> ChainBlockRange:
    chain_id = get_chain_id(w3)
    finalized_block = get_finalized_block_number(w3)

    start_block_number = resolve_first_block_at_or_after(
        w3,
        start_timestamp,
        finalized_block,
    )

    end_block_number = resolve_last_block_at_or_before(
        w3,
        end_timestamp,
        finalized_block,
    )

    if start_block_number > end_block_number:
        raise ValueError(
            "Resolved start block is after resolved end block."
        )

    start_block = get_block_metadata(w3, start_block_number)
    end_block = get_block_metadata(w3, end_block_number)

    return ChainBlockRange(
        chain_id=chain_id,
        start=start_block,
        end=end_block,
        finality_rule="finalized block tag",
        rpc_method="eth_getBlockByNumber",
    )


def build_manifest(
    ethereum_range: ChainBlockRange,
    arbitrum_range: ChainBlockRange,
) -> dict:
    return {
        "experiment": "pilot_24h",
        "time_window": {
            "start": START_TIME.isoformat().replace("+00:00", "Z"),
            "end": END_TIME.isoformat().replace("+00:00", "Z"),
        },
        "block_resolution": {
            "timestamp_semantics": {
                "start": "first block with timestamp >= start_timestamp",
                "end": "last block with timestamp <= end_timestamp",
            },
            "chains": {
                "ethereum": asdict(ethereum_range),
                "arbitrum": asdict(arbitrum_range),
            },
        },
    }


def save_manifest(manifest: dict) -> Path:
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    output_path = MANIFEST_DIR / "pilot_24h_block_range.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)

    return output_path


def main() -> None:
    eth_rpc_url = os.getenv("ETH_RPC_URL")
    arb_rpc_url = os.getenv("ARB_RPC_URL")

    if not eth_rpc_url:
        raise RuntimeError("ETH_RPC_URL is not configured.")

    if not arb_rpc_url:
        raise RuntimeError("ARB_RPC_URL is not configured.")

    print("Connecting to Ethereum...")
    eth_w3 = create_web3(eth_rpc_url)

    print("Connecting to Arbitrum...")
    arb_w3 = create_web3(arb_rpc_url)

    print("Resolving Ethereum block range...")
    ethereum_range = resolve_chain_range(
        eth_w3,
        int(START_TIME.timestamp()),
        int(END_TIME.timestamp()),
    )

    print("Resolving Arbitrum block range...")
    arbitrum_range = resolve_chain_range(
        arb_w3,
        int(START_TIME.timestamp()),
        int(END_TIME.timestamp()),
    )

    manifest = build_manifest(
        ethereum_range,
        arbitrum_range,
    )

    manifest_path = save_manifest(manifest)

    print("\nBlock Resolution Complete")
    print("=" * 60)

    for name, block_range in (
        ("Ethereum", ethereum_range),
        ("Arbitrum", arbitrum_range),
    ):
        print(f"\n{name}")
        print(f"Chain ID : {block_range.chain_id}")
        print(
            f"Start    : "
            f"{block_range.start.number} "
            f"{block_range.start.hash}"
        )
        print(
            f"End      : "
            f"{block_range.end.number} "
            f"{block_range.end.hash}"
        )

    print(f"\nManifest: {manifest_path}")


if __name__ == "__main__":
    main()