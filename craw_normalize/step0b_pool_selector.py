from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from web3 import Web3


load_dotenv()

DEFAULT_REGISTRY = Path("craw_normalize/discovered_pools.json")
DEFAULT_BLOCK_MANIFEST = Path("artifact/manifests/block_range_8w.json")
DEFAULT_OUTPUT = Path("artifact/manifests/locked_pools_24.json")

CHAIN_ORDER = ("ethereum", "arbitrum", "base")
RPC_ENV_BY_CHAIN = {
    "ethereum": "ETH_RPC_URL",
    "arbitrum": "ARB_RPC_URL",
    "base": "BASE_RPC_URL",
}

TRAIN_DAYS = 7
MAX_POOLS_PER_CHAIN = 8
MAX_POOLS_TOTAL = 24
DEFAULT_CHUNK_SIZE = 10_000
DEFAULT_MIN_CHUNK_SIZE = 250
DEFAULT_MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 60

SWAP_TOPIC = Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lock the most active pools using Swap logs from only the first "
            "seven days of the 8-week cohort."
        )
    )
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument(
        "--block-manifest", type=Path, default=DEFAULT_BLOCK_MANIFEST
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE
    )
    parser.add_argument(
        "--min-chunk-size", type=int, default=DEFAULT_MIN_CHUNK_SIZE
    )
    parser.add_argument(
        "--max-retries", type=int, default=DEFAULT_MAX_RETRIES
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Required input does not exist: {path}")
    with path.open("r", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_cli_limits(args: argparse.Namespace) -> None:
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    if args.chunk_size > DEFAULT_CHUNK_SIZE:
        raise ValueError(
            f"--chunk-size cannot exceed {DEFAULT_CHUNK_SIZE:,} blocks"
        )
    if args.min_chunk_size <= 0:
        raise ValueError("--min-chunk-size must be positive")
    if args.min_chunk_size > args.chunk_size:
        raise ValueError("--min-chunk-size cannot exceed --chunk-size")
    if args.max_retries <= 0:
        raise ValueError("--max-retries must be positive")


def validate_inputs(
    registry: dict[str, Any], block_manifest: dict[str, Any]
) -> None:
    pools = registry.get("pools")
    if not isinstance(pools, list) or not pools:
        raise ValueError("Pool registry has no non-empty 'pools' list")

    manifest_chains = block_manifest.get("block_resolution", {}).get("chains")
    if not isinstance(manifest_chains, dict):
        raise ValueError("Block manifest has no block_resolution.chains object")

    missing_chains = [chain for chain in CHAIN_ORDER if chain not in manifest_chains]
    if missing_chains:
        raise ValueError(f"Block manifest is missing chains: {missing_chains}")

    seen: set[tuple[str, str]] = set()
    required = {
        "chain",
        "chain_id",
        "dex_family",
        "pool_address",
        "token0_contract",
        "token1_contract",
        "fee_tier",
    }
    for pool in pools:
        missing = required - set(pool)
        if missing:
            raise ValueError(f"Pool entry is missing fields: {sorted(missing)}")
        if pool["chain"] not in CHAIN_ORDER:
            raise ValueError(f"Unsupported pool chain: {pool['chain']}")
        if pool["dex_family"] != "uniswap_v3":
            raise ValueError(
                f"Unsupported DEX family for {pool['pool_address']}: "
                f"{pool['dex_family']}"
            )

        address = Web3.to_checksum_address(pool["pool_address"])
        key = (pool["chain"], address.lower())
        if key in seen:
            raise ValueError(f"Duplicate pool in registry: {key}")
        seen.add(key)

        expected_chain_id = int(manifest_chains[pool["chain"]]["chain_id"])
        if int(pool["chain_id"]) != expected_chain_id:
            raise ValueError(
                f"Chain ID mismatch for {pool['pool_address']}: "
                f"registry={pool['chain_id']}, manifest={expected_chain_id}"
            )


def create_web3(chain_name: str) -> Web3:
    rpc_env = RPC_ENV_BY_CHAIN[chain_name]
    rpc_url = os.getenv(rpc_env)
    if not rpc_url:
        raise RuntimeError(f"{rpc_env} is not configured")

    w3 = Web3(
        Web3.HTTPProvider(
            rpc_url,
            request_kwargs={"timeout": REQUEST_TIMEOUT_SECONDS},
        )
    )
    if not w3.is_connected():
        raise ConnectionError(f"Cannot connect to {chain_name} RPC")
    return w3


def backoff(attempt: int) -> None:
    delay = (2 ** (attempt - 1)) * random.uniform(0.5, 1.5)
    time.sleep(delay)


def get_block_with_retry(
    w3: Web3, block_identifier: int, max_retries: int
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return w3.eth.get_block(block_identifier)
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                backoff(attempt)
    assert last_error is not None
    raise last_error


def verify_locked_start_block(
    w3: Web3,
    chain_name: str,
    chain_range: dict[str, Any],
    max_retries: int,
) -> None:
    start = chain_range["start"]
    block = get_block_with_retry(w3, int(start["number"]), max_retries)
    if block["hash"].hex().lower() != str(start["hash"]).lower():
        raise RuntimeError(
            f"{chain_name}: locked start block hash mismatch; refusing to select"
        )
    if int(block["timestamp"]) != int(start["timestamp"]):
        raise RuntimeError(f"{chain_name}: locked start block timestamp mismatch")


def resolve_last_block_at_or_before(
    w3: Web3,
    target_timestamp: int,
    low_block: int,
    high_block: int,
    max_retries: int,
) -> int:
    """Resolve the inclusive training end inside the locked cohort range."""
    low = low_block
    high = high_block
    while low < high:
        mid = (low + high + 1) // 2
        block = get_block_with_retry(w3, mid, max_retries)
        if int(block["timestamp"]) <= target_timestamp:
            low = mid
        else:
            high = mid - 1

    block = get_block_with_retry(w3, low, max_retries)
    if int(block["timestamp"]) > target_timestamp:
        raise RuntimeError("No block exists at or before the training end")
    return low


def get_logs_with_retry(
    w3: Web3,
    addresses: list[str],
    start_block: int,
    end_block: int,
    max_retries: int,
) -> tuple[list[Any], int]:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            logs = w3.eth.get_logs(
                {
                    "address": addresses,
                    "fromBlock": start_block,
                    "toBlock": end_block,
                    "topics": [SWAP_TOPIC],
                }
            )
            return list(logs), attempt
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                backoff(attempt)
    assert last_error is not None
    raise last_error


def count_swap_logs(
    w3: Web3,
    chain_name: str,
    pools: list[dict[str, Any]],
    start_block: int,
    end_block: int,
    initial_chunk_size: int,
    min_chunk_size: int,
    max_retries: int,
) -> tuple[dict[str, int], dict[str, int]]:
    """Count all pool logs, reducing block ranges rejected by the RPC."""
    addresses = [Web3.to_checksum_address(pool["pool_address"]) for pool in pools]
    counts = Counter({address.lower(): 0 for address in addresses})
    metrics = {
        "rpc_requests": 0,
        "rpc_retries": 0,
        "successful_chunks": 0,
        "chunk_reductions": 0,
    }

    current = start_block
    chunk_size = initial_chunk_size
    while current <= end_block:
        target = min(current + chunk_size - 1, end_block)
        try:
            logs, attempts = get_logs_with_retry(
                w3, addresses, current, target, max_retries
            )
        except Exception as exc:
            metrics["rpc_requests"] += max_retries
            metrics["rpc_retries"] += max_retries - 1
            attempted_size = target - current + 1
            if attempted_size <= min_chunk_size:
                raise RuntimeError(
                    f"{chain_name}: eth_getLogs failed for minimum range "
                    f"[{current}, {target}]"
                ) from exc
            chunk_size = max(min_chunk_size, attempted_size // 2)
            metrics["chunk_reductions"] += 1
            print(
                f"[{chain_name}] RPC rejected [{current}, {target}]; "
                f"reducing chunk size to {chunk_size}"
            )
            continue

        metrics["rpc_requests"] += attempts
        metrics["rpc_retries"] += attempts - 1
        metrics["successful_chunks"] += 1
        for log in logs:
            address = str(log["address"]).lower()
            if address not in counts:
                raise RuntimeError(
                    f"{chain_name}: RPC returned a log for an unrequested pool: "
                    f"{address}"
                )
            counts[address] += 1

        print(
            f"[{chain_name}] blocks {current}-{target}: "
            f"{len(logs)} Swap logs"
        )
        current = target + 1

    return dict(counts), metrics


def block_metadata(block: Any) -> dict[str, Any]:
    return {
        "number": int(block["number"]),
        "hash": block["hash"].hex(),
        "timestamp": int(block["timestamp"]),
    }


def rank_pools(
    pools: list[dict[str, Any]], counts: dict[str, int]
) -> list[dict[str, Any]]:
    ranked = []
    for pool in pools:
        row = dict(pool)
        row["pool_address"] = Web3.to_checksum_address(row["pool_address"])
        row["swap_count"] = int(counts[row["pool_address"].lower()])
        ranked.append(row)

    ranked.sort(
        key=lambda pool: (-pool["swap_count"], pool["pool_address"].lower())
    )
    for rank, pool in enumerate(ranked, start=1):
        pool["activity_rank_within_chain"] = rank
    return ranked


def ensure_output_is_unlocked(path: Path) -> None:
    """Refuse a rerun before making any RPC request."""
    if path.exists():
        raise FileExistsError(
            f"Locked output already exists: {path}. "
            "It is immutable; use a new versioned path for a documented rerun."
        )


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    """Atomically create the locked manifest without replacing an old one."""
    ensure_output_is_unlocked(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2, ensure_ascii=False)
        file.write("\n")
    temporary.replace(path)


def main() -> None:
    args = parse_args()
    validate_cli_limits(args)
    ensure_output_is_unlocked(args.output)

    registry = load_json(args.registry)
    block_manifest = load_json(args.block_manifest)
    validate_inputs(registry, block_manifest)

    cohort_start = parse_utc(block_manifest["time_window"]["start"])
    cohort_end = parse_utc(block_manifest["time_window"]["end"])
    train_end = cohort_start + timedelta(days=TRAIN_DAYS) - timedelta(seconds=1)
    if train_end >= cohort_end:
        raise ValueError("Seven-day training period must end before the test period")

    chain_ranges = block_manifest["block_resolution"]["chains"]
    all_selected: list[dict[str, Any]] = []
    chain_results: dict[str, Any] = {}

    for chain_name in CHAIN_ORDER:
        chain_range = chain_ranges[chain_name]
        chain_pools = [
            pool for pool in registry["pools"] if pool["chain"] == chain_name
        ]
        if not chain_pools:
            raise ValueError(f"No discovered pools for {chain_name}")

        print(f"\n[{chain_name}] Connecting and validating locked cohort...")
        w3 = create_web3(chain_name)
        expected_chain_id = int(chain_range["chain_id"])
        actual_chain_id = int(w3.eth.chain_id)
        if actual_chain_id != expected_chain_id:
            raise RuntimeError(
                f"{chain_name}: RPC chain ID {actual_chain_id} does not match "
                f"manifest chain ID {expected_chain_id}"
            )

        verify_locked_start_block(
            w3, chain_name, chain_range, args.max_retries
        )
        train_start_block = int(chain_range["start"]["number"])
        train_end_block = resolve_last_block_at_or_before(
            w3,
            int(train_end.timestamp()),
            train_start_block,
            int(chain_range["end"]["number"]),
            args.max_retries,
        )
        counts, rpc_metrics = count_swap_logs(
            w3,
            chain_name,
            chain_pools,
            train_start_block,
            train_end_block,
            args.chunk_size,
            args.min_chunk_size,
            args.max_retries,
        )
        ranked = rank_pools(chain_pools, counts)
        selected = ranked[:MAX_POOLS_PER_CHAIN]
        all_selected.extend(selected)

        end_block = get_block_with_retry(
            w3, train_end_block, args.max_retries
        )
        chain_results[chain_name] = {
            "chain_id": expected_chain_id,
            "training_block_range": {
                "start": dict(chain_range["start"]),
                "end": block_metadata(end_block),
                "timestamp_semantics": {
                    "start": "first cohort block with timestamp >= train_start",
                    "end": "last cohort block with timestamp <= train_end",
                },
            },
            "discovered_pool_count": len(ranked),
            "selected_pool_count": len(selected),
            "rpc_metrics": rpc_metrics,
            "selected_pools": selected,
        }

    if len(all_selected) > MAX_POOLS_TOTAL:
        raise AssertionError(
            f"Selection produced {len(all_selected)} pools; limit is {MAX_POOLS_TOTAL}"
        )
    for chain_name in CHAIN_ORDER:
        selected_count = sum(pool["chain"] == chain_name for pool in all_selected)
        if selected_count > MAX_POOLS_PER_CHAIN:
            raise AssertionError(
                f"{chain_name} selection exceeds {MAX_POOLS_PER_CHAIN} pools"
            )

    output = {
        "experiment": "xmev_cal",
        "manifest_type": "locked_pool_selection",
        "status": "locked",
        "created_at_utc": isoformat_z(datetime.now(timezone.utc)),
        "scientific_integrity": {
            "selection_data": "first_7_days_of_cohort_only",
            "test_period_event_logs_used_for_selection": False,
            "ranking_metric": "uniswap_v3_swap_event_count",
            "ranking_order": "swap_count_desc_then_pool_address_asc",
        },
        "training_period": {
            "start": isoformat_z(cohort_start),
            "end": isoformat_z(train_end),
            "duration_days": TRAIN_DAYS,
        },
        "test_period": {
            "start": isoformat_z(train_end + timedelta(seconds=1)),
            "end": isoformat_z(cohort_end),
            "excluded_from_pool_selection": True,
        },
        "limits": {
            "max_pools_per_chain": MAX_POOLS_PER_CHAIN,
            "max_pools_total": MAX_POOLS_TOTAL,
        },
        "log_query": {
            "rpc_method": "eth_getLogs",
            "event": "Swap(address,address,int256,int256,uint160,uint128,int24)",
            "topic0": SWAP_TOPIC,
            "initial_chunk_size": args.chunk_size,
            "minimum_chunk_size": args.min_chunk_size,
            "max_retries_per_range": args.max_retries,
            "address_batching": "all_discovered_pools_within_each_chain",
        },
        "inputs": {
            "discovered_pools": {
                "path": args.registry.as_posix(),
                "sha256": sha256_file(args.registry),
            },
            "block_range": {
                "path": args.block_manifest.as_posix(),
                "sha256": sha256_file(args.block_manifest),
            },
        },
        "selected_pool_count": len(all_selected),
        "chains": chain_results,
        "pools": all_selected,
    }

    write_json_atomic(args.output, output)
    print(f"\nLocked {len(all_selected)} pools in {args.output}")


if __name__ == "__main__":
    main()
