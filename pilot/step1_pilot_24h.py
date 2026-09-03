from __future__ import annotations

import hashlib
import json
import os
import platform
import random
import shutil
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
from dotenv import load_dotenv
from eth_abi import decode
from web3 import Web3


load_dotenv()

ARTIFACT_DIR = Path("artifact")
MANIFEST_DIR = ARTIFACT_DIR / "manifests"
RAW_DIR = ARTIFACT_DIR / "raw" / "pilot_24h"
PARQUET_DIR = ARTIFACT_DIR / "parquet"
RESULT_DIR = ARTIFACT_DIR / "results"

BLOCK_MANIFEST = MANIFEST_DIR / "pilot_24h_block_range.json"
CONFIG_MANIFEST = MANIFEST_DIR / "pilot_24h_config.json"
CHECKPOINT_FILE = RESULT_DIR / "pilot_24h_checkpoint.json"
RUNTIME_LOG = RESULT_DIR / "pilot_24h_runtime.jsonl"
SUMMARY_FILE = RESULT_DIR / "pilot_24h_summary.json"

# 10,000 is used as the initial value
INITIAL_CHUNK_SIZE = 10000
MIN_CHUNK_SIZE = 2000
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 30

SWAP_TOPIC = Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()

ACROSS_FUNDS_DEPOSITED_SIGNATURE = (
    "FundsDeposited(bytes32,bytes32,uint256,uint256,uint256,uint256,"
    "uint32,uint32,uint32,bytes32,bytes32,bytes32,bytes)"
)
ACROSS_FILLED_RELAY_SIGNATURE = (
    "FilledRelay(bytes32,bytes32,uint256,uint256,uint256,uint256,"
    "uint256,uint256,uint256,uint256,uint256,bytes32)"
)

# ACROSS_FUNDS_TOPIC = Web3.keccak(
#     text=ACROSS_FUNDS_DEPOSITED_SIGNATURE
# ).hex()
# ACROSS_FILLED_TOPIC = Web3.keccak(
#     text=ACROSS_FILLED_RELAY_SIGNATURE
# ).hex()

ACROSS_FUNDS_TOPIC = "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
ACROSS_FILLED_TOPIC = "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"

CONFIG = {
    "experiment": "pilot_24h",
    "chain_pair": ["ethereum", "arbitrum"],
    "pools": [
        {
            "chain": "ethereum",
            "chain_id": 1,
            "dex_family": "uniswap_v3",
            "address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            "pair": "USDC/WETH",
            "fee_tier": 500,
        },
        {
            "chain": "arbitrum",
            "chain_id": 42161,
            "dex_family": "uniswap_v3",
            "address": "0xC6962004f452bE9203591991D15f6b388e09E8D0",
            "pair": "USDC/WETH",
            "fee_tier": 500,
        },
    ],
    "bridge": {
        "family": "across",
        "contracts": [
            {
                "chain": "ethereum",
                "chain_id": 1,
                "role": "spoke_pool",
                "address": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
            },
            {
                "chain": "arbitrum",
                "chain_id": 42161,
                "role": "spoke_pool",
                "address": "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A",
            },
        ],
        "queries": {
            "deposit_event": ACROSS_FUNDS_DEPOSITED_SIGNATURE,
            "fill_event": ACROSS_FILLED_RELAY_SIGNATURE,
        },
    },
    "etl": {
        "rpc_method": "eth_getLogs",
        "initial_chunk_size": INITIAL_CHUNK_SIZE,
        "minimum_chunk_size": MIN_CHUNK_SIZE,
        "max_retries_per_chunk": MAX_RETRIES,
        "backoff": "exponential_backoff_with_jitter",
        "workers": 1,
        "compression": "zstd",
        "checkpoint_after_successful_chunk": True,
        "raw_chunk_sha256": True,
    },
    "candidate_generation_thresholds": {
        "amount_tolerance": 0.01,
        "latency_quantile": 0.95
    },
}

TOKEN_MAP = {
    # Ethereum
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"asset_class": "USDC", "decimals": 6},
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"asset_class": "WETH", "decimals": 18},
    # Arbitrum
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": {"asset_class": "USDC", "decimals": 6},
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {"asset_class": "WETH", "decimals": 18},
}

def get_token_info(address_bytes: bytes) -> tuple[str | None, int]:
    hex_addr = "0x" + address_bytes[-20:].hex().lower()
    if hex_addr in TOKEN_MAP:
        return TOKEN_MAP[hex_addr]["asset_class"], TOKEN_MAP[hex_addr]["decimals"]
    return None, 18 

@dataclass
class RuntimeState:
    started_at: str
    peak_rss_bytes: int = 0
    rpc_requests: int = 0
    rpc_failures: int = 0
    retries: int = 0
    rows: int = 0
    chunks_successful: int = 0
    chunks_failed: int = 0
    swaps: int = 0
    bridge_logs: int = 0

STATE = RuntimeState(started_at=datetime.now(timezone.utc).isoformat())

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def update_peak_rss() -> None:
    rss = psutil.Process(os.getpid()).memory_info().rss
    if rss > STATE.peak_rss_bytes:
        STATE.peak_rss_bytes = rss


def disk_usage_bytes(path: Path) -> int:
    path.mkdir(parents=True, exist_ok=True)
    return sum(f.stat().st_size for f in path.rglob('*') if f.is_file())


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(value, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp.replace(path)


def append_runtime(event: dict[str, Any]) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **event,
    }
    with RUNTIME_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_block_manifest() -> dict[str, Any]:
    if not BLOCK_MANIFEST.exists():
        raise FileNotFoundError(
            f"Missing locked block manifest: {BLOCK_MANIFEST}"
        )
    return json.loads(BLOCK_MANIFEST.read_text(encoding="utf-8"))


def freeze_config(block_manifest: dict[str, Any]) -> dict[str, Any]:
    config = {
        **CONFIG,
        "block_resolution": block_manifest["block_resolution"],
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
    }
    return config


def validate_config(config: dict[str, Any]) -> None:
    if config["chain_pair"] != ["ethereum", "arbitrum"]:
        raise ValueError("Pilot chain pair is not the locked Ethereum/Arbitrum pair.")

    if len(config["pools"]) != 2:
        raise ValueError("Pilot must contain exactly two pools.")

    if len(config["bridge"]["contracts"]) != 2:
        raise ValueError("Across bridge must contain origin/destination SpokePool contracts.")

    for pool in config["pools"]:
        Web3.to_checksum_address(pool["address"])

    for contract in config["bridge"]["contracts"]:
        Web3.to_checksum_address(contract["address"])

    if not 2_000 <= INITIAL_CHUNK_SIZE <= 10_000:
        raise ValueError("Initial chunk size must be in the protocol's 2,000–10,000 range.")


def save_frozen_config(config: dict[str, Any]) -> None:
    # Hash the protocol-relevant configuration before execution.
    canonical = json.dumps(
        config,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    config["config_sha256"] = sha256_bytes(canonical)
    write_json_atomic(CONFIG_MANIFEST, config)


def create_web3(rpc_env: str) -> Web3:
    rpc_url = os.getenv(rpc_env)
    if not rpc_url:
        raise RuntimeError(f"{rpc_env} is not configured.")

    return Web3(
        Web3.HTTPProvider(
            rpc_url,
            request_kwargs={"timeout": REQUEST_TIMEOUT_SECONDS},
        )
    )


def load_checkpoint() -> dict[str, Any]:
    if not CHECKPOINT_FILE.exists():
        return {"chunks": {}}
    return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))


def checkpoint(
    chain_name: str,
    query_name: str,
    start_block: int,
    end_block: int,
    raw_path: Path,
    parquet_path: Path,
    row_count: int,
    attempt_count: int,
) -> None:
    data = load_checkpoint()
    key = f"{chain_name}:{query_name}:{start_block}:{end_block}"
    data["chunks"][key] = {
        "chain": chain_name,
        "query": query_name,
        "start_block": start_block,
        "end_block": end_block,
        "status": "success",
        "attempt_count": attempt_count,
        "raw_path": str(raw_path),
        "raw_sha256": sha256_file(raw_path),
        "parquet_path": str(parquet_path),
        "parquet_sha256": sha256_file(parquet_path),
        "rows": row_count,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(CHECKPOINT_FILE, data)


def backoff_sleep(attempt: int) -> None:
    delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    delay *= random.uniform(0.5, 1.5)
    time.sleep(delay)


def rpc_get_logs(
    w3: Web3,
    chain_name: str,
    address: str,
    start_block: int,
    end_block: int,
    topic: str,
    query_name: str,
) -> tuple[list[Any], int]:
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        STATE.rpc_requests += 1
        update_peak_rss()

        try:
            logs = w3.eth.get_logs(
                {
                    "address": Web3.to_checksum_address(address),
                    "fromBlock": start_block,
                    "toBlock": end_block,
                    "topics": [topic],
                }
            )
            append_runtime(
                {
                    "event": "rpc_success",
                    "chain": chain_name,
                    "query": query_name,
                    "start_block": start_block,
                    "end_block": end_block,
                    "attempt": attempt,
                    "rows": len(logs),
                }
            )
            return list(logs), attempt

        except Exception as exc:
            last_error = exc
            STATE.rpc_failures += 1
            STATE.retries += 1

            append_runtime(
                {
                    "event": "rpc_error",
                    "chain": chain_name,
                    "query": query_name,
                    "start_block": start_block,
                    "end_block": end_block,
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                    "error": repr(exc),
                }
            )

            if attempt < MAX_RETRIES:
                backoff_sleep(attempt)

    assert last_error is not None
    raise last_error


def raw_log_to_json(log: Any) -> dict[str, Any]:
    return {
        "address": Web3.to_checksum_address(log["address"]),
        "blockNumber": int(log["blockNumber"]),
        "transactionHash": log["transactionHash"].hex(),
        "transactionIndex": int(log["transactionIndex"]),
        "blockHash": log["blockHash"].hex(),
        "logIndex": int(log["logIndex"]),
        "data": log["data"].hex(),
        "topics": [topic.hex() for topic in log["topics"]],
    }


def write_raw_chunk(
    chain_name: str,
    query_name: str,
    start_block: int,
    end_block: int,
    logs: list[Any],
) -> Path:
    path = (
        RAW_DIR
        / f"chain={chain_name}"
        / f"query={query_name}"
        / f"{start_block}-{end_block}.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as fh:
        for log in logs:
            fh.write(json.dumps(raw_log_to_json(log), sort_keys=True) + "\n")

    return path


def get_block_context(w3: Web3, block_numbers: list[int]) -> dict[int, Any]:
    result: dict[int, Any] = {}
    for block_number in sorted(set(block_numbers)):
        last_error = None
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                block = w3.eth.get_block(block_number)
                result[block_number] = block
                update_peak_rss()
                break 
                
            except Exception as exc:
                last_error = exc
                STATE.rpc_failures += 1
                STATE.retries += 1
                
                print(f"  [FALSE] At {block_number}: {exc}. Retrying {attempt}/{MAX_RETRIES}...")
                
                if attempt < MAX_RETRIES:
                    backoff_sleep(attempt)
                    
        if block_number not in result and last_error is not None:
            raise last_error
            
    return result


def decode_uniswap_swap(
    w3: Web3,
    chain_id: int,
    pool: str,
    log: Any,
    blocks: dict[int, Any],
) -> dict[str, Any]:
    amount0, amount1, sqrt_price_x96, liquidity, tick = decode(
        ["int256", "int256", "uint160", "uint128", "int24"],
        bytes(log["data"]),
    )

    sender = w3.to_checksum_address(log["topics"][1][-20:])
    recipient = w3.to_checksum_address(log["topics"][2][-20:])
    block = blocks[int(log["blockNumber"])]

    asset_out = None
    amount_out = None
    if amount0 < 0:
        asset_out = "USDC"
        amount_out = abs(amount0) / (10 ** 6)
    elif amount1 < 0:
        asset_out = "WETH"
        amount_out = abs(amount1) / (10 ** 18)

    return {
        "chain_id": chain_id,
        "block_number": int(log["blockNumber"]),
        "block_hash": log["blockHash"].hex(),
        "timestamp": int(block["timestamp"]),
        "tx_hash": log["transactionHash"].hex(),
        "log_index": int(log["logIndex"]),
        "pool": Web3.to_checksum_address(pool),
        "dex_family": "uniswap_v3",
        "actor_eoa": sender,
        "first_called_contract": None,
        "beneficiary": recipient,
        "token_in_id": None,
        "asset_in": None,
        "amount_in": None,
        "token_out_id": None,
        "asset_out": asset_out,
        "amount_out": amount_out,
        "gas_native": None,
        "decoder_version": "uniswap_v3_swap_v1",
        "data_quality": "partial_raw_swap",
        "amount0": str(amount0),
        "amount1": str(amount1),
        "sqrt_price_x96": str(sqrt_price_x96),
        "liquidity": str(liquidity),
        "tick": int(tick),
    }


def normalize_swap_chunk(
    w3: Web3,
    pool: dict[str, Any],
    logs: list[Any],
) -> list[dict[str, Any]]:
    blocks = get_block_context(
        w3,
        [int(log["blockNumber"]) for log in logs],
    )

    return [
        decode_uniswap_swap(
            w3,
            pool["chain_id"],
            pool["address"],
            log,
            blocks,
        )
        for log in logs
    ]

def decode_across_bridge(
    w3: Web3,
    chain_id: int,
    log: Any,
    blocks: dict[int, Any],
    query_name: str,
) -> dict[str, Any]:
    block = blocks[int(log["blockNumber"])]
    timestamp = int(block["timestamp"])
    tx_hash = log["transactionHash"].hex()
    
    record = {
        "bridge_family": "across",
        "message_id": None,
        "src_chain": None,
        "dst_chain": None,
        "send_tx": None,
        "receive_tx": None,
        "asset_class": None,  
        "amount_sent": None,
        "amount_received": None,
        "bridge_fee": None,
        "send_time": None,
        "receive_time": None,
        "provenance_type": "exact_log",
        "status": "success",
        "abi_version": "across_v3",
    }

    if query_name == "across_funds_deposited":
        data_types = ["bytes32", "bytes32", "uint256", "uint256", "uint32", "uint32", "uint32", "bytes32", "bytes32", "bytes"]
        decoded_data = decode(data_types, bytes(log["data"]))
        
        asset_class, decimals = get_token_info(decoded_data[0]) 
        raw_amount_sent = decoded_data[2]
        
        record["message_id"] = str(int.from_bytes(log["topics"][2], byteorder='big'))
        record["src_chain"] = chain_id
        record["dst_chain"] = int.from_bytes(log["topics"][1], byteorder='big')
        record["send_tx"] = tx_hash
        record["send_time"] = timestamp
        record["asset_class"] = asset_class
        record["amount_sent"] = str(raw_amount_sent / (10 ** decimals)) if asset_class else str(raw_amount_sent)
        
    elif query_name == "across_filled_relay":
        data_types = ["bytes32", "bytes32", "uint256", "uint256", "uint256", "uint32", "uint32", "bytes32", "bytes32", "bytes32", "bytes32", "(bytes32,bytes32,uint256,uint8)"]
        decoded_data = decode(data_types, bytes(log["data"]))
        
        asset_class, decimals = get_token_info(decoded_data[1]) 
        raw_amount_received = decoded_data[3]
        
        record["message_id"] = str(int.from_bytes(log["topics"][2], byteorder='big'))
        record["src_chain"] = int.from_bytes(log["topics"][1], byteorder='big')
        record["dst_chain"] = chain_id
        record["receive_tx"] = tx_hash
        record["receive_time"] = timestamp
        record["asset_class"] = asset_class
        record["amount_received"] = str(raw_amount_received / (10 ** decimals)) if asset_class else str(raw_amount_received)
        
    return record

def normalize_bridge_chunk(
    w3: Web3,
    chain_id: int,
    query_name: str,
    logs: list[Any],
) -> list[dict[str, Any]]:
    blocks = get_block_context(
        w3,
        [int(log["blockNumber"]) for log in logs],
    )
    return [
        decode_across_bridge(w3, chain_id, log, blocks, query_name)
        for log in logs
    ]

def write_parquet(rows: list[dict[str, Any]], output_path: Path) -> None:
    # Import only when needed; avoids pandas entirely.
    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, output_path, compression="zstd")

def process_query(
    w3: Web3,
    chain_name: str,
    chain_id: int,
    address: str,
    start_block: int,
    end_block: int,
    topic: str,
    query_name: str,
    record_type: str,
    pool_config: dict[str, Any] | None = None,
) -> None:
    data = load_checkpoint()
    max_completed_block = start_block - 1
    prefix = f"{chain_name}:{query_name}:"
    
    for key, value in data.get("chunks", {}).items():
        if key.startswith(prefix) and value["status"] == "success":
            if value["end_block"] > max_completed_block:
                max_completed_block = value["end_block"]
                
    current = max_completed_block + 1
    
    if current > end_block:
        print(f"-> Skip {query_name} on {chain_name} because it's already completed.")
        return
    else:
        print(f"-> Continue {query_name} on {chain_name} from block {current}...")

    chunk_size = INITIAL_CHUNK_SIZE

    while current <= end_block:
        target = min(current + chunk_size - 1, end_block)

        append_runtime({
            "event": "chunk_start",
            "chain": chain_name,
            "query": query_name,
            "start_block": current,
            "end_block": target,
            "chunk_size": chunk_size,
            "peak_rss_bytes": STATE.peak_rss_bytes,
        })

        try:
            logs, attempts = rpc_get_logs(w3, chain_name, address, current, target, topic, query_name)
        except Exception:
            STATE.chunks_failed += 1
            if chunk_size > MIN_CHUNK_SIZE:
                new_size = max(MIN_CHUNK_SIZE, chunk_size // 2)
                append_runtime({
                    "event": "chunk_resize",
                    "chain": chain_name,
                    "query": query_name,
                    "old_chunk_size": chunk_size,
                    "new_chunk_size": new_size,
                    "reason": "rpc_error",
                })
                chunk_size = new_size
                continue
            raise

        raw_path = write_raw_chunk(chain_name, query_name, current, target, logs)
        rows: list[dict[str, Any]] = []
        parquet_path = raw_path
        
        if record_type != "none":
            if record_type == "swap" and pool_config is not None:
                parquet_path = PARQUET_DIR / f"swaps/chain_id={chain_id}/year_month=2025-09/part-{chain_name}-{query_name}-{current}-{target}.parquet"
                if logs:
                    rows = normalize_swap_chunk(w3, pool_config, logs)
                    write_parquet(rows, parquet_path)
                else:
                    parquet_path = PARQUET_DIR / "swaps" / "_empty"
            
            elif record_type == "bridge":
                parquet_path = PARQUET_DIR / f"bridge/bridge_family=across/year_month=2025-09/part-{chain_name}-{query_name}-{current}-{target}.parquet"
                if logs:
                    rows = normalize_bridge_chunk(w3, chain_id, query_name, logs)
                    write_parquet(rows, parquet_path)
                else:
                    parquet_path = PARQUET_DIR / "bridge" / "_empty"

        checkpoint(
            chain_name, query_name, current, target, raw_path,
            parquet_path if parquet_path != raw_path and parquet_path.exists() else raw_path,
            len(rows) if record_type != "none" else len(logs),
            attempts,
        )

        STATE.rows += len(rows) if record_type != "none" else len(logs)
        if record_type == "swap":
            STATE.swaps += len(rows)
        elif record_type == "bridge":
            STATE.bridge_logs += len(rows)
        else:
            STATE.bridge_logs += len(logs)

        STATE.chunks_successful += 1
        update_peak_rss()

        append_runtime({
            "event": "chunk_success",
            "chain": chain_name,
            "query": query_name,
            "start_block": current,
            "end_block": target,
            "chunk_size": chunk_size,
            "attempt_count": attempts,
            "raw_rows": len(logs),
            "normalized_rows": len(rows),
            "raw_sha256": sha256_file(raw_path),
            "peak_rss_bytes": STATE.peak_rss_bytes,
        })

        current = target + 1

def build_summary(
    started_monotonic: float,
    config: dict[str, Any],
) -> dict[str, Any]:
    elapsed = time.monotonic() - started_monotonic
    disk_used = disk_usage_bytes(ARTIFACT_DIR)

    return {
        "experiment": "pilot_24h",
        "status": "completed",
        "observed": {
            "wall_time_seconds": elapsed,
            "rows": STATE.rows,
            "swap_rows": STATE.swaps,
            "bridge_logs": STATE.bridge_logs,
            "rpc_requests": STATE.rpc_requests,
            "rpc_failures": STATE.rpc_failures,
            "retries": STATE.retries,
            "successful_chunks": STATE.chunks_successful,
            "failed_chunks": STATE.chunks_failed,
            "peak_rss_bytes": STATE.peak_rss_bytes,
            "artifact_disk_used_bytes": disk_used,
        },
        "candidate_rate": {
            "status": "not_computed",
            "candidates": None,
            "swaps": STATE.swaps,
            "candidates_per_million_swaps": None,
            "reason": "Calculate at Step 3", 
        },
        "environment": config["environment"],
        "config_sha256": config["config_sha256"],
    }

def main() -> None:
    started_monotonic = time.monotonic()

    for directory in (MANIFEST_DIR, RAW_DIR, PARQUET_DIR, RESULT_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    block_manifest = load_block_manifest()
    config = freeze_config(block_manifest)
    validate_config(config)
    save_frozen_config(config)

    eth_w3 = create_web3("ETH_RPC_URL")
    arb_w3 = create_web3("ARB_RPC_URL")

    if not eth_w3.is_connected():
        raise ConnectionError("Cannot connect to Ethereum RPC.")
    if not arb_w3.is_connected():
        raise ConnectionError("Cannot connect to Arbitrum RPC.")

    eth_range = block_manifest["block_resolution"]["chains"]["ethereum"]
    arb_range = block_manifest["block_resolution"]["chains"]["arbitrum"]

    eth_pool = config["pools"][0]
    arb_pool = config["pools"][1]

    process_query(
        eth_w3,
        "ethereum",
        1,
        eth_pool["address"],
        eth_range["start"]["number"],
        eth_range["end"]["number"],
        SWAP_TOPIC,
        "uniswap_v3_swap",
        record_type="swap",
        pool_config=eth_pool,
    )

    process_query(
        arb_w3,
        "arbitrum",
        42161,
        arb_pool["address"],
        arb_range["start"]["number"],
        arb_range["end"]["number"],
        SWAP_TOPIC,
        "uniswap_v3_swap",
        record_type="swap",
        pool_config=arb_pool,
    )

    # Bridge logs are extracted separately and kept raw for the next
    # normalization/candidate stage.
    for chain_name, chain_id, w3, chain_range, bridge in (
        (
            "ethereum",
            1,
            eth_w3,
            eth_range,
            config["bridge"]["contracts"][0],
        ),
        (
            "arbitrum",
            42161,
            arb_w3,
            arb_range,
            config["bridge"]["contracts"][1],
        ),
    ):
        process_query(
            w3,
            chain_name,
            chain_id,
            bridge["address"],
            chain_range["start"]["number"],
            chain_range["end"]["number"],
            ACROSS_FUNDS_TOPIC,
            "across_funds_deposited",
            record_type="bridge",
        )

        process_query(
            w3,
            chain_name,
            chain_id,
            bridge["address"],
            chain_range["start"]["number"],
            chain_range["end"]["number"],
            ACROSS_FILLED_TOPIC,
            "across_filled_relay",
            record_type="bridge",
        )

    summary = build_summary(started_monotonic, config)
    write_json_atomic(SUMMARY_FILE, summary)

    print(json.dumps(summary, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        update_peak_rss()
        append_runtime(
            {
                "event": "fatal_error",
                "error_type": type(exc).__name__,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
                "peak_rss_bytes": STATE.peak_rss_bytes,
            }
        )
        raise
