from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import random
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psutil
from dotenv import load_dotenv
from eth_abi import decode
from web3 import Web3

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("etl_8w")

ARTIFACT_DIR    = Path("artifact")
MANIFEST_DIR    = ARTIFACT_DIR / "manifests"
RAW_DIR         = ARTIFACT_DIR / "raw" / "etl_8w"
PARQUET_DIR     = ARTIFACT_DIR / "parquet"
RESULT_DIR      = ARTIFACT_DIR / "results"

CONFIG_FILE     = MANIFEST_DIR / "8w_config.json"
POOLS_FILE      = MANIFEST_DIR / "locked_pools_24.json"
BLOCKS_FILE     = MANIFEST_DIR / "block_range_8w.json"

CHECKPOINT_FILE = RESULT_DIR / "etl_8w_checkpoint.json"
RUNTIME_LOG     = RESULT_DIR / "etl_8w_runtime.jsonl"
SUMMARY_FILE    = RESULT_DIR / "etl_8w_summary.json"
FROZEN_CFG_FILE = MANIFEST_DIR / "etl_8w_frozen_config.json"

# These parameters are loaded dynamically from locked_pools_24.json["log_query"]
# at runtime inside load_configs(); they start as None and are set by init_rpc_params().
SWAP_TOPIC:          str = ""
INITIAL_CHUNK_SIZE:  int = 0
MIN_CHUNK_SIZE:      int = 0
MAX_RETRIES:         int = 0
BACKOFF_BASE_SECONDS:    float = 1.0
REQUEST_TIMEOUT_SECONDS: int   = 30

@dataclass
class RuntimeState:
    started_at: str
    peak_rss_bytes: int     = 0
    rpc_requests:   int     = 0
    rpc_failures:   int     = 0
    retries:        int     = 0
    chunks_ok:      int     = 0
    chunks_failed:  int     = 0
    swap_rows:      int     = 0
    bridge_rows:    int     = 0
    failed_tasks: list[str] = field(default_factory=list)


STATE = RuntimeState(started_at=datetime.now(timezone.utc).isoformat())


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def update_peak_rss() -> None:
    rss = psutil.Process(os.getpid()).memory_info().rss
    if rss > STATE.peak_rss_bytes:
        STATE.peak_rss_bytes = rss


def disk_usage_bytes(path: Path) -> int:
    path.mkdir(parents=True, exist_ok=True)
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def write_json_atomic(path: Path, value: Any) -> None:
    """Write JSON to a tmp file then atomically rename to prevent corruption."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def append_runtime(event: dict[str, Any]) -> None:
    """Append one JSON-line record to the runtime log."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    record = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
    with RUNTIME_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def init_rpc_params(pools_manifest: dict) -> None:
    """Read RPC query parameters from locked_pools_24.json["log_query"] into globals."""
    global SWAP_TOPIC, INITIAL_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_RETRIES
    lq = pools_manifest.get("log_query", {})
    SWAP_TOPIC         = lq["topic0"]
    INITIAL_CHUNK_SIZE = int(lq["initial_chunk_size"])
    MIN_CHUNK_SIZE     = int(lq["minimum_chunk_size"])
    MAX_RETRIES        = int(lq["max_retries_per_range"])
    log.info(
        f"RPC params loaded from locked_pools_24.json | topic0={SWAP_TOPIC} "
        f"| initial_chunk={INITIAL_CHUNK_SIZE} | min_chunk={MIN_CHUNK_SIZE} "
        f"| max_retries={MAX_RETRIES}"
    )


# Per-chain Web3 instances needed by the on-chain decimals fetcher.
# Populated by build_web3_pool() and stored here for use in get_token_info().
_W3_BY_CHAIN: dict[str, "Web3"] = {}
_ERC20_ABI: list = []   # set from cfg["erc20_abi"] in load_configs()


def init_erc20_abi(cfg: dict) -> None:
    """Load ERC-20 ABI from 8w_config.json into module-level _ERC20_ABI."""
    global _ERC20_ABI
    _ERC20_ABI = cfg.get("erc20_abi", [])
    log.info(f"ERC-20 ABI loaded from 8w_config.json ({len(_ERC20_ABI)} items)")


def load_configs() -> tuple[dict, dict, dict]:
    """Load and return (8w_config, locked_pools_24, block_range_8w)."""
    for fp in (CONFIG_FILE, POOLS_FILE, BLOCKS_FILE):
        if not fp.exists():
            raise FileNotFoundError(f"Required manifest missing: {fp}")
    cfg    = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    pools  = json.loads(POOLS_FILE.read_text(encoding="utf-8"))
    blocks = json.loads(BLOCKS_FILE.read_text(encoding="utf-8"))
    init_rpc_params(pools)
    init_erc20_abi(cfg)
    return cfg, pools, blocks


def build_token_map(cfg: dict) -> dict[str, dict]:
    """Build lowercase-address -> {asset_class, chain_name, decimals=None} from chains config.

    ``decimals`` is intentionally left as None here; it will be populated
    lazily via ``fetch_decimals_onchain()`` on the first time each token is seen.
    """
    token_map: dict[str, dict] = {}
    for chain_name, chain_cfg in cfg["chains"].items():
        for symbol, tok in chain_cfg["tokens"].items():
            token_map[tok["address"].lower()] = {
                "asset_class": symbol,
                "chain_name":  chain_name,
                "decimals":    None,   # populated on-chain lazily
            }
    return token_map


def fetch_decimals_onchain(
    address_lower: str, chain_name: str = "", w3: Web3 | None = None
) -> int:
    """Call decimals() on the ERC-20 contract and return the result.

    Falls back to 18 if the call fails (e.g. native ETH address or broken ABI).
    Uses the provided provider or a provider from ``_W3_BY_CHAIN``.
    """
    provider = w3 or _W3_BY_CHAIN.get(chain_name)
    if provider is None and _W3_BY_CHAIN:
        provider = next(iter(_W3_BY_CHAIN.values()))

    if provider is None or not _ERC20_ABI:
        log.warning(
            f"  [DECIMALS] Provider or ABI unavailable for {address_lower} "
            f"on {chain_name} — falling back to 18"
        )
        return 18
    try:
        checksum = Web3.to_checksum_address(address_lower)
        contract = provider.eth.contract(address=checksum, abi=_ERC20_ABI)
        dec = int(contract.functions.decimals().call())
        log.info(f"  [DECIMALS ON-CHAIN] {address_lower} ({chain_name}) -> {dec}")
        return dec
    except Exception as exc:
        log.warning(
            f"  [DECIMALS] on-chain call decimals() failed for {address_lower} "
            f"on {chain_name}: {exc!r} — falling back to 18"
        )
        return 18


def get_token_info(
    address_lower: str, token_map: dict, chain_name: str = "", w3: Web3 | None = None
) -> tuple:
    """Return (asset_class, decimals) for a token address.

    If ``decimals`` is not yet cached in ``token_map``, it is fetched from
    the on-chain ``decimals()`` call and stored in ``token_map`` for future look-ups.
    """
    info = token_map.get(address_lower)
    if info is None:
        # Token not in our known set; fetch decimals on-chain and cache it.
        dec = fetch_decimals_onchain(address_lower, chain_name=chain_name, w3=w3)
        token_map[address_lower] = {
            "asset_class": None,
            "chain_name":  chain_name,
            "decimals":    dec,
        }
        return None, dec
    if info.get("decimals") is None:
        # Known token but decimals not yet fetched; query on-chain and cache.
        target_chain = chain_name or info.get("chain_name", "")
        dec = fetch_decimals_onchain(address_lower, chain_name=target_chain, w3=w3)
        info["decimals"] = dec
        log.debug(
            f"  [DECIMALS] cached {address_lower} decimals={dec} (chain={target_chain})"
        )
    return info["asset_class"], info["decimals"]

@dataclass
class ETLTask:
    """One atomic unit of work: one address x one event topic on one chain."""
    chain_name:    str
    chain_id:      int
    address:       str
    start_block:   int
    end_block:     int
    topic:         str
    query_name:    str
    record_type:   str
    bridge_family: str | None
    event_name:    str | None
    pool_meta:     dict | None


def build_task_plan(cfg: dict, pools_manifest: dict, blocks_manifest: dict) -> list[ETLTask]:
    """
    Build the complete ordered list of ETLTask from JSON manifests.
    No chain names, addresses, or topic hashes are hardcoded here.
    """
    tasks: list[ETLTask] = []
    chain_ranges = blocks_manifest["block_resolution"]["chains"]

    # Swap tasks: 24 pools x 1 Swap event
    for pool in pools_manifest["pools"]:
        chain = pool["chain"]
        if chain not in chain_ranges:
            continue
        rng = chain_ranges[chain]
        short = pool["pool_address"][:10].lower()
        tasks.append(ETLTask(
            chain_name    = chain,
            chain_id      = pool["chain_id"],
            address       = pool["pool_address"],
            start_block   = rng["start"]["number"],
            end_block     = rng["end"]["number"],
            topic         = SWAP_TOPIC,
            query_name    = f"swap_{short}",
            record_type   = "swap",
            bridge_family = None,
            event_name    = "Swap",
            pool_meta     = pool,
        ))

    # Bridge tasks: deduplicate by (src_chain, family, contract, event_name)
    seen: set[tuple] = set()
    for bridge in cfg["bridges"]:
        family = bridge["bridge_family"]
        src    = bridge["src_chain"]
        addr   = bridge["bridge_contract"]
        for ev_name, topic_hex in bridge["events_topic0"].items():
            key = (src, family, addr.lower(), ev_name)
            if key in seen:
                continue
            seen.add(key)
            if src not in chain_ranges:
                continue
            rng   = chain_ranges[src]
            short = addr[:10].lower()
            tasks.append(ETLTask(
                chain_name    = src,
                chain_id      = cfg["chains"][src]["chain_id"],
                address       = addr,
                start_block   = rng["start"]["number"],
                end_block     = rng["end"]["number"],
                topic         = topic_hex,
                query_name    = f"{family}_{ev_name.lower()}_{short}",
                record_type   = "bridge",
                bridge_family = family,
                event_name    = ev_name,
                pool_meta     = None,
            ))
    return tasks


def _task_ck_key(t: ETLTask) -> str:
    return f"TASK:{t.chain_name}:{t.query_name}"


def log_startup_status(tasks: list[ETLTask], checkpoint: dict) -> None:
    """
    At startup, print status of all tasks (done/pending) grouped by chain.
    Pending tasks show block ranges so the operator knows what is left.
    """
    done_keys = {
        k for k, v in checkpoint.get("chunks", {}).items()
        if v.get("status") == "success" and v.get("fully_complete")
    }
    total   = len(tasks)
    done    = sum(1 for t in tasks if _task_ck_key(t) in done_keys)
    pending = total - done

    log.info("=" * 70)
    log.info("ETL 8-week  |  experiment: cohort_8w")
    log.info(f"  Total tasks: {total}  |  Done: {done}  |  Pending: {pending}")
    log.info("  Pending task details by chain:")

    chain_stats: dict[str, dict] = {}
    for t in tasks:
        cs = chain_stats.setdefault(t.chain_name, {"total": 0, "done": 0, "pending": []})
        cs["total"] += 1
        if _task_ck_key(t) in done_keys:
            cs["done"] += 1
        else:
            cs["pending"].append(
                f"      [{t.record_type}|{t.bridge_family or 'dex'}] "
                f"{t.query_name}  blocks={t.start_block:,}-{t.end_block:,}"
            )

    for ch, cs in chain_stats.items():
        p = cs["total"] - cs["done"]
        log.info(f"    chain={ch}: {cs['total']} tasks | done={cs['done']} | pending={p}")
        for line in cs["pending"]:
            log.info(line)

    log.info("=" * 70)

def build_web3_pool(cfg: dict) -> dict[str, Web3]:
    """Create and verify one Web3 instance per chain defined in config.

    Also populates the module-level ``_W3_BY_CHAIN`` dict so that
    ``fetch_decimals_onchain`` can reach the right provider.
    """
    global _W3_BY_CHAIN
    pool: dict[str, Web3] = {}
    for chain_name, chain_cfg in cfg["chains"].items():
        rpc_env = chain_cfg["rpc_env"]
        rpc_url = os.getenv(rpc_env)
        if not rpc_url:
            raise RuntimeError(f"RPC env var '{rpc_env}' for chain '{chain_name}' is not set.")
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": REQUEST_TIMEOUT_SECONDS}))
        if not w3.is_connected():
            raise ConnectionError(f"Cannot connect to {chain_name} RPC ({rpc_env}).")
        log.info(f"  Connected: chain={chain_name} (chain_id={chain_cfg['chain_id']})")
        pool[chain_name] = w3
        _W3_BY_CHAIN[chain_name] = w3
    return pool


def load_checkpoint() -> dict[str, Any]:
    if not CHECKPOINT_FILE.exists():
        return {"chunks": {}}
    return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))


def checkpoint_chunk(
    chain_name: str, query_name: str,
    start_block: int, end_block: int,
    raw_path: Path, parquet_path,
    row_count: int, attempt_count: int,
) -> None:
    """Persist a successfully processed chunk atomically."""
    data = load_checkpoint()
    key  = f"{chain_name}:{query_name}:{start_block}:{end_block}"
    entry: dict[str, Any] = {
        "chain": chain_name, "query": query_name,
        "start_block": start_block, "end_block": end_block,
        "status": "success", "attempt_count": attempt_count,
        "raw_path": str(raw_path), "raw_sha256": sha256_file(raw_path),
        "rows": row_count, "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    if parquet_path and parquet_path.exists():
        entry["parquet_path"]   = str(parquet_path)
        entry["parquet_sha256"] = sha256_file(parquet_path)
    data["chunks"][key] = entry
    write_json_atomic(CHECKPOINT_FILE, data)


def checkpoint_task_complete(task: ETLTask) -> None:
    """Mark a full task (all chunks) as complete in checkpoint."""
    data = load_checkpoint()
    data["chunks"][_task_ck_key(task)] = {
        "status": "success", "fully_complete": True,
        "query_name": task.query_name, "chain": task.chain_name,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(CHECKPOINT_FILE, data)


def checkpoint_task_failed(task: ETLTask, error: str) -> None:
    """Record a permanently failed task; never fabricate missing data."""
    data = load_checkpoint()
    data["chunks"][_task_ck_key(task) + ":FAILED"] = {
        "status": "failed", "query_name": task.query_name, "chain": task.chain_name,
        "error": error, "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(CHECKPOINT_FILE, data)


def get_resume_block(chain_name: str, query_name: str, start_block: int) -> int:
    """Return the next unprocessed block for this task."""
    data   = load_checkpoint()
    prefix = f"{chain_name}:{query_name}:"
    max_end = start_block - 1
    for key, val in data.get("chunks", {}).items():
        if key.startswith(prefix) and val.get("status") == "success":
            if val["end_block"] > max_end:
                max_end = val["end_block"]
    return max_end + 1


def backoff_sleep(attempt: int) -> None:
    delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) * random.uniform(0.5, 1.5)
    time.sleep(delay)


class ChunkSizeError(Exception):
    """Raised by rpc_get_logs when the RPC indicates the block range is too wide.

    Signals ``process_task`` to halve the chunk size rather than exhaust retries.
    Concretely triggered by HTTP 429, ReadTimeout, ConnectionTimeout, and provider
    range-limit messages (e.g. "query returned more than X results").
    """


# Keywords found in provider error messages that indicate a range-limit hit.
_RANGE_LIMIT_PHRASES: tuple[str, ...] = (
    "query returned more than",
    "block range too large",
    "eth_getLogs is limited to",
    "eth_getlogs is limited to",
    "log query timeout",
    "max results",
    "result window is too large",
    "exceed maximum",
    "range too wide",
    "exceeds max results",
    "query exceeds max block range",
    "range exceeds",
    "block range exceeds",
    "response size should not exceed",
    "response size exceeded",
    "statement timeout",
    "-32005",
)


def _is_chunk_size_error(exc: BaseException) -> bool:
    """Return True if ``exc`` should trigger a chunk-size reduction.

    Concretely captures HTTP 429, ReadTimeout, Timeout, and provider
    block-range limit errors.
    """
    # Import lazily so this module stays importable without requests installed.
    try:
        import requests
        if isinstance(exc, (
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        )):
            return True
        if isinstance(exc, requests.exceptions.HTTPError):
            if exc.response is not None and exc.response.status_code == 429:
                return True
    except ImportError:
        pass
    if isinstance(exc, TimeoutError):
        return True
    # Timeout-like class names (web3 or custom provider wrappers)
    exc_name = type(exc).__name__.lower()
    if "timeout" in exc_name or "readtimeout" in exc_name:
        return True

    # Check error message
    exc_str = f"{exc} {repr(exc)}".lower()
    if any(
        marker in exc_str
        for marker in ("429", "too many requests", "rate limit", "timeout", "readtimeout")
    ):
        return True

    # Provider range-limit messages
    if any(phrase in exc_str for phrase in _RANGE_LIMIT_PHRASES):
        return True
    return False


def rpc_get_logs(
    w3: Web3, chain_name: str, address: str,
    start_block: int, end_block: int, topic: str, query_name: str,
) -> tuple[list[Any], int]:
    """eth_getLogs with exponential backoff + jitter; returns (logs, attempts).

    Raises:
        ChunkSizeError: if the RPC signals HTTP 429, timeout, or a block-range
            limit.  ``process_task`` catches this to halve the chunk without
            consuming all retry budget.
        Exception: any other logic or connection failure after ``MAX_RETRIES`` attempts.
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        STATE.rpc_requests += 1
        update_peak_rss()
        try:
            logs = w3.eth.get_logs({
                "address": Web3.to_checksum_address(address),
                "fromBlock": start_block, "toBlock": end_block, "topics": [topic],
            })
            append_runtime({
                "event": "rpc_success", "chain": chain_name, "query": query_name,
                "start_block": start_block, "end_block": end_block,
                "attempt": attempt, "rows": len(logs),
            })
            return list(logs), attempt
        except Exception as exc:
            last_error = exc
            STATE.rpc_failures += 1
            STATE.retries      += 1
            append_runtime({
                "event": "rpc_error", "chain": chain_name, "query": query_name,
                "start_block": start_block, "end_block": end_block,
                "attempt": attempt, "error_type": type(exc).__name__, "error": repr(exc),
            })
            # Chunk-size errors (rate-limit / timeout / range-limit) bypass the
            # normal retry loop and are raised immediately so the caller can
            # halve the range instead of wasting retries.
            if _is_chunk_size_error(exc):
                log.warning(
                    f"  [RPC RANGE/TIMEOUT] {query_name} ({start_block}-{end_block}): "
                    f"triggered chunk resize on {type(exc).__name__}: {exc}"
                )
                raise ChunkSizeError(repr(exc)) from exc

            # Other errors (logic, connection errors) retry up to MAX_RETRIES
            if attempt < MAX_RETRIES:
                log.info(
                    f"  [RPC RETRY] {query_name} ({start_block}-{end_block}) "
                    f"attempt {attempt}/{MAX_RETRIES} failed ({type(exc).__name__}), retrying..."
                )
                backoff_sleep(attempt)
    assert last_error is not None
    raise last_error


def get_block_timestamps(w3: Web3, block_numbers: list[int]) -> dict[int, int]:
    """Return {block_number: unix_timestamp} for a batch of block numbers."""
    result: dict[int, int] = {}
    for bn in sorted(set(block_numbers)):
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                blk = w3.eth.get_block(bn)
                result[bn] = int(blk["timestamp"])
                update_peak_rss()
                break
            except Exception as exc:
                last_err = exc
                STATE.rpc_failures += 1
                STATE.retries      += 1
                if attempt < MAX_RETRIES:
                    backoff_sleep(attempt)
        if bn not in result:
            assert last_err is not None
            raise last_err
    return result

def raw_log_to_dict(lg: Any) -> dict[str, Any]:
    return {
        "address":          Web3.to_checksum_address(lg["address"]),
        "blockNumber":      int(lg["blockNumber"]),
        "transactionHash":  lg["transactionHash"].hex(),
        "transactionIndex": int(lg["transactionIndex"]),
        "blockHash":        lg["blockHash"].hex(),
        "logIndex":         int(lg["logIndex"]),
        "data":             lg["data"].hex() if isinstance(lg["data"], (bytes, bytearray)) else lg["data"],
        "topics":           [t.hex() if isinstance(t, (bytes, bytearray)) else t for t in lg["topics"]],
    }


def write_raw_chunk(
    chain_name: str, query_name: str,
    start_block: int, end_block: int, logs: list[Any],
) -> Path:
    path = RAW_DIR / f"chain={chain_name}" / f"query={query_name}" / f"{start_block}-{end_block}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for lg in logs:
            fh.write(json.dumps(raw_log_to_dict(lg), sort_keys=True) + "\n")
    return path


PARQUET_BATCH_SIZE = 500   # rows per RecordBatch flush to disk


def write_parquet_streaming(
    rows_iter: "Iterable[dict[str, Any]]",
    output_path: Path,
    batch_size: int = PARQUET_BATCH_SIZE,
) -> int:
    """Write an iterable of dicts to ZSTD Parquet using a streaming ParquetWriter.

    Rows are accumulated into small RecordBatches of ``batch_size`` and flushed
    to disk immediately — no full in-memory array is built.

    Returns the total number of rows written.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer: "pq.ParquetWriter | None" = None
    schema: "pa.Schema | None"        = None
    batch_buf: list[dict[str, Any]]   = []
    total_rows = 0

    def _flush(buf: list[dict[str, Any]]) -> None:
        nonlocal writer, schema
        if not buf:
            return
        table = pa.Table.from_pylist(buf)
        if writer is None:
            schema = table.schema
            writer = pq.ParquetWriter(str(output_path), schema, compression="zstd")
        else:
            # Cast to the established schema so column order / types are stable.
            table = table.cast(schema)
        writer.write_table(table)

    try:
        for row in rows_iter:
            batch_buf.append(row)
            if len(batch_buf) >= batch_size:
                _flush(batch_buf)
                total_rows += len(batch_buf)
                batch_buf = []
        if batch_buf:
            _flush(batch_buf)
            total_rows += len(batch_buf)
            batch_buf = []
    finally:
        if writer is not None:
            writer.close()

    return total_rows


def build_parquet_path(
    record_type: str, chain_id: int, bridge_family,
    query_name: str, start_block: int, end_block: int, ref_timestamp,
) -> Path:
    """Compute partitioned Parquet path: chain_id=X/year_month=YYYY-MM/part-*.parquet"""
    if ref_timestamp:
        ym = datetime.fromtimestamp(ref_timestamp, tz=timezone.utc).strftime("%Y-%m")
    else:
        ym = "unknown"
    part = f"part-{query_name}-{start_block}-{end_block}.parquet"
    if record_type == "swap":
        return PARQUET_DIR / "swaps" / f"chain_id={chain_id}" / f"year_month={ym}" / part
    else:
        fam = bridge_family or "unknown"
        return PARQUET_DIR / "bridge" / f"bridge_family={fam}" / f"chain_id={chain_id}" / f"year_month={ym}" / part

def decode_uniswap_v3_swap(
    w3: Web3, pool: dict, lg: Any, timestamps: dict[int, int], token_map: dict,
    chain_name: str = "",
) -> dict[str, Any]:
    """Decode a Uniswap V3 Swap log into a normalized row."""
    amount0, amount1, sqrt_price_x96, liquidity, tick = decode(
        ["int256", "int256", "uint160", "uint128", "int24"], bytes(lg["data"]),
    )
    sender    = w3.to_checksum_address(lg["topics"][1][-20:])
    recipient = w3.to_checksum_address(lg["topics"][2][-20:])
    ts        = timestamps[int(lg["blockNumber"])]

    asset0, dec0 = get_token_info(pool["token0_contract"].lower(), token_map, chain_name)
    asset1, dec1 = get_token_info(pool["token1_contract"].lower(), token_map, chain_name)

    asset_out = amount_out = None
    if amount0 < 0:
        asset_out  = asset0
        amount_out = abs(amount0) / (10 ** dec0)
    elif amount1 < 0:
        asset_out  = asset1
        amount_out = abs(amount1) / (10 ** dec1)

    return {
        "chain_id":        pool["chain_id"],
        "block_number":    int(lg["blockNumber"]),
        "block_hash":      lg["blockHash"].hex(),
        "timestamp":       ts,
        "tx_hash":         lg["transactionHash"].hex(),
        "log_index":       int(lg["logIndex"]),
        "pool":            Web3.to_checksum_address(pool["pool_address"]),
        "dex_family":      pool.get("dex_family", "uniswap_v3"),
        "fee_tier":        pool.get("fee_tier"),
        "sender":          sender,
        "recipient":       recipient,
        "token0":          pool["token0_contract"],
        "token1":          pool["token1_contract"],
        "asset0":          asset0,
        "asset1":          asset1,
        "asset_out":       asset_out,
        "amount_out":      amount_out,
        "amount0":         str(amount0),
        "amount1":         str(amount1),
        "sqrt_price_x96":  str(sqrt_price_x96),
        "liquidity":       str(liquidity),
        "tick":            int(tick),
        "decoder":         "uniswap_v3_swap_v1",
    }


def _addr_hex(raw: bytes) -> str:
    return "0x" + raw[-20:].hex()


def _decode_cctp(
    chain_id: int, chain_name: str, event_name: str,
    lg: Any, tx_hash: str, ts: int, token_map: dict,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "bridge_family": "cctp", "event_name": event_name,
        "chain_id": chain_id, "chain_name": chain_name,
        "tx_hash": tx_hash, "timestamp": ts,
        "block_number": int(lg["blockNumber"]), "log_index": int(lg["logIndex"]),
        "decoder": "cctp_v1",
    }
    try:
        if event_name == "DepositForBurn":
            decoded = decode(
                ["uint256", "bytes32", "uint32", "bytes32", "bytes32"], bytes(lg["data"]),
            )
            rec["nonce"]              = int.from_bytes(lg["topics"][1], "big")
            rec["burn_token"]         = _addr_hex(lg["topics"][2])
            rec["depositor"]          = _addr_hex(lg["topics"][3])
            rec["amount_raw"]         = str(decoded[0])
            rec["mint_recipient"]     = "0x" + decoded[1].hex()
            rec["destination_domain"] = decoded[2]
            asset, dec = get_token_info(rec["burn_token"].lower(), token_map, chain_name)
            rec["asset_class"]  = asset
            rec["amount_human"] = str(decoded[0] / (10 ** dec)) if asset else None
        elif event_name == "MessageReceived":
            decoded = decode(["uint32", "bytes32", "bytes"], bytes(lg["data"]))
            rec["caller"]        = _addr_hex(lg["topics"][1])
            rec["nonce"]         = int.from_bytes(lg["topics"][2], "big")
            rec["source_domain"] = decoded[0]
            rec["sender"]        = "0x" + decoded[1].hex()
    except Exception as e:
        rec["decode_error"] = repr(e)
    return rec


def _decode_across(
    chain_id: int, chain_name: str, event_name: str,
    lg: Any, tx_hash: str, ts: int, token_map: dict,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "bridge_family": "across", "event_name": event_name,
        "chain_id": chain_id, "chain_name": chain_name,
        "tx_hash": tx_hash, "timestamp": ts,
        "block_number": int(lg["blockNumber"]), "log_index": int(lg["logIndex"]),
        "decoder": "across_v3",
    }
    try:
        if event_name == "V3FundsDeposited":
            decoded = decode(
                ["address", "address", "uint256", "uint256",
                 "uint32", "uint32", "uint32",
                 "address", "address", "bytes"],
                bytes(lg["data"]),
            )
            rec["dst_chain_id"]         = int.from_bytes(lg["topics"][1], "big")
            rec["deposit_id"]           = int.from_bytes(lg["topics"][2], "big")
            rec["depositor"]            = _addr_hex(lg["topics"][3])
            rec["input_token"]          = decoded[0]
            rec["output_token"]         = decoded[1]
            rec["input_amount_raw"]     = str(decoded[2])
            rec["output_amount_raw"]    = str(decoded[3])
            rec["quote_timestamp"]      = decoded[4]
            rec["fill_deadline"]        = decoded[5]
            rec["exclusivity_deadline"] = decoded[6]
            rec["recipient"]            = decoded[7]
            rec["exclusive_relayer"]    = decoded[8]
            asset, dec = get_token_info(decoded[0].lower(), token_map, chain_name)
            rec["asset_class"]        = asset
            rec["input_amount_human"] = str(decoded[2] / (10 ** dec)) if asset else None
        elif event_name == "FilledV3Relay":
            decoded = decode(
                ["address", "address", "uint256", "uint256",
                 "uint256", "uint32", "uint32",
                 "address", "address", "address", "bytes",
                 "(address,bytes,uint256,uint8)"],
                bytes(lg["data"]),
            )
            rec["src_chain_id"]          = int.from_bytes(lg["topics"][1], "big")
            rec["deposit_id"]            = int.from_bytes(lg["topics"][2], "big")
            rec["relayer"]               = _addr_hex(lg["topics"][3])
            rec["input_token"]           = decoded[0]
            rec["output_token"]          = decoded[1]
            rec["input_amount_raw"]      = str(decoded[2])
            rec["output_amount_raw"]     = str(decoded[3])
            rec["repayment_chain_id"]    = decoded[4]
            rec["fill_deadline"]         = decoded[5]
            rec["exclusivity_deadline"]  = decoded[6]
            rec["exclusive_relayer"]     = decoded[7]
            rec["depositor"]             = decoded[8]
            rec["recipient"]             = decoded[9]
            asset, dec = get_token_info(decoded[1].lower(), token_map, chain_name)
            rec["asset_class"]         = asset
            rec["output_amount_human"] = str(decoded[3] / (10 ** dec)) if asset else None
    except Exception as e:
        rec["decode_error"] = repr(e)
    return rec


def _decode_stargate(
    chain_id: int, chain_name: str, event_name: str,
    lg: Any, tx_hash: str, ts: int, token_map: dict,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "bridge_family": "stargate", "event_name": event_name,
        "chain_id": chain_id, "chain_name": chain_name,
        "tx_hash": tx_hash, "timestamp": ts,
        "block_number": int(lg["blockNumber"]), "log_index": int(lg["logIndex"]),
        "decoder": "stargate_oft_v1",
    }
    try:
        if event_name == "OFTSent":
            decoded = decode(["uint32", "uint256", "uint256"], bytes(lg["data"]))
            rec["guid"]               = lg["topics"][1].hex()
            rec["from_address"]       = _addr_hex(lg["topics"][2])
            rec["dst_eid"]            = decoded[0]
            rec["amount_sent_ld"]     = str(decoded[1])
            rec["amount_received_ld"] = str(decoded[2])
        elif event_name == "OFTReceived":
            decoded = decode(["uint32", "uint256"], bytes(lg["data"]))
            rec["guid"]               = lg["topics"][1].hex()
            rec["to_address"]         = _addr_hex(lg["topics"][2])
            rec["src_eid"]            = decoded[0]
            rec["amount_received_ld"] = str(decoded[1])
    except Exception as e:
        rec["decode_error"] = repr(e)
    return rec


def decode_bridge_log(
    chain_id: int, chain_name: str, bridge_family: str, event_name: str,
    lg: Any, timestamps: dict[int, int], token_map: dict,
) -> dict[str, Any]:
    """Dispatch to the correct bridge decoder by family."""
    ts      = timestamps[int(lg["blockNumber"])]
    tx_hash = lg["transactionHash"].hex()
    if bridge_family == "cctp":
        return _decode_cctp(chain_id, chain_name, event_name, lg, tx_hash, ts, token_map)
    elif bridge_family == "across":
        return _decode_across(chain_id, chain_name, event_name, lg, tx_hash, ts, token_map)
    elif bridge_family == "stargate":
        return _decode_stargate(chain_id, chain_name, event_name, lg, tx_hash, ts, token_map)
    else:
        return {
            "bridge_family": bridge_family, "event_name": event_name,
            "chain_id": chain_id, "tx_hash": tx_hash, "timestamp": ts,
            "raw_data": lg["data"].hex() if hasattr(lg["data"], "hex") else str(lg["data"]),
            "decoder": "raw_passthrough",
        }

def process_task(task: ETLTask, w3: Web3, token_map: dict) -> None:
    """
    Process all blocks for one ETLTask in chunks.
    Each chunk: fetch logs -> write raw JSONL -> decode -> write Parquet -> checkpoint.
    Chunk halved on RPC error; raises if minimum chunk size also fails.
    """
    current = get_resume_block(task.chain_name, task.query_name, task.start_block)

    if current > task.end_block:
        log.info(f"  [SKIP] {task.chain_name}/{task.query_name} already complete")
        return

    log.info(
        f"  [START] chain={task.chain_name} | {task.record_type}"
        f" | query={task.query_name}"
        f" | blocks {current:,}-{task.end_block:,}"
        f" | contract={task.address}"
    )

    chunk_size  = INITIAL_CHUNK_SIZE
    part_ref_ts = None

    while current <= task.end_block:
        target = min(current + chunk_size - 1, task.end_block)

        append_runtime({
            "event": "chunk_start", "chain": task.chain_name, "query": task.query_name,
            "start_block": current, "end_block": target,
            "chunk_size": chunk_size, "peak_rss_bytes": STATE.peak_rss_bytes,
        })

        try:
            logs, attempts = rpc_get_logs(
                w3, task.chain_name, task.address,
                current, target, task.topic, task.query_name,
            )
        except ChunkSizeError as exc:
            # Rate-limit / timeout / range-limit: always reduce chunk size.
            STATE.chunks_failed += 1
            new_sz = max(MIN_CHUNK_SIZE, chunk_size // 2)
            log.warning(
                f"  [RESIZE/RATE] {task.query_name}: {chunk_size}->{new_sz} blocks "
                f"({type(exc).__name__}: {exc})"
            )
            append_runtime({
                "event": "chunk_resize", "chain": task.chain_name,
                "query": task.query_name,
                "old_size": chunk_size, "new_size": new_sz,
                "reason": "chunk_size_error",
            })
            chunk_size = new_sz
            continue
        except Exception as exc:
            # Logic / connection errors: only reduce chunk if there is room left,
            # otherwise propagate so the caller can mark the task as failed.
            STATE.chunks_failed += 1
            if chunk_size > MIN_CHUNK_SIZE:
                new_sz = max(MIN_CHUNK_SIZE, chunk_size // 2)
                log.warning(
                    f"  [RESIZE/CHUNK] {task.query_name}: {chunk_size}->{new_sz} blocks "
                    f"({type(exc).__name__}: {exc})"
                )
                append_runtime({
                    "event": "chunk_resize", "chain": task.chain_name,
                    "query": task.query_name,
                    "old_size": chunk_size, "new_size": new_sz, "reason": "rpc_error",
                })
                chunk_size = new_sz
                continue
            log.error(
                f"  [CHUNK FAILED] {task.query_name}: already at minimum chunk size ({MIN_CHUNK_SIZE}) "
                f"and failed with {type(exc).__name__}: {exc}"
            )
            raise

        raw_path = write_raw_chunk(
            task.chain_name, task.query_name, current, target, logs
        )

        parquet_path = None
        decoded_rows = 0

        if logs:
            timestamps = get_block_timestamps(w3, [int(lg["blockNumber"]) for lg in logs])
            if part_ref_ts is None:
                part_ref_ts = min(timestamps.values())

            # Build a generator so decoded rows are never all in memory at once.
            if task.record_type == "swap" and task.pool_meta is not None:
                def _row_gen(
                    _logs=logs, _pool=task.pool_meta,
                    _ts=timestamps, _tm=token_map, _cn=task.chain_name,
                ):
                    for _lg in _logs:
                        yield decode_uniswap_v3_swap(
                            w3, _pool, _lg, _ts, _tm, chain_name=_cn
                        )
            elif task.record_type == "bridge":
                ev = task.event_name or "unknown"
                def _row_gen(
                    _logs=logs, _ts=timestamps, _tm=token_map,
                    _ev=ev,
                ):
                    for _lg in _logs:
                        yield decode_bridge_log(
                            task.chain_id, task.chain_name,
                            task.bridge_family or "unknown", _ev,
                            _lg, _ts, _tm,
                        )
            else:
                _row_gen = None  # type: ignore[assignment]

            if _row_gen is not None:
                parquet_path = build_parquet_path(
                    task.record_type, task.chain_id, task.bridge_family,
                    task.query_name, current, target, part_ref_ts,
                )
                decoded_rows = write_parquet_streaming(_row_gen(), parquet_path)
                if task.record_type == "swap":
                    STATE.swap_rows += decoded_rows
                else:
                    STATE.bridge_rows += decoded_rows

        checkpoint_chunk(
            task.chain_name, task.query_name,
            current, target,
            raw_path, parquet_path,
            decoded_rows if decoded_rows else len(logs),
            attempts,
        )
        STATE.chunks_ok += 1
        update_peak_rss()

        total_range = max(1, task.end_block - task.start_block)
        pct = (current - task.start_block) / total_range * 100
        log.info(
            f"  [CHUNK OK] {task.chain_name}/{task.query_name} "
            f"blocks {current:,}-{target:,} "
            f"| logs={len(logs)} rows={decoded_rows} "
            f"| progress={pct:.1f}%"
        )
        append_runtime({
            "event": "chunk_success", "chain": task.chain_name, "query": task.query_name,
            "start_block": current, "end_block": target,
            "attempt_count": attempts, "raw_rows": len(logs),
            "decoded_rows": decoded_rows, "peak_rss_bytes": STATE.peak_rss_bytes,
        })

        current = target + 1


def build_summary(started_monotonic: float) -> dict[str, Any]:
    elapsed   = time.monotonic() - started_monotonic
    disk_used = disk_usage_bytes(ARTIFACT_DIR)
    return {
        "experiment": "cohort_8w",
        "status": "completed" if not STATE.failed_tasks else "partial",
        "observed": {
            "wall_time_seconds":   elapsed,
            "swap_rows":           STATE.swap_rows,
            "bridge_rows":         STATE.bridge_rows,
            "rpc_requests":        STATE.rpc_requests,
            "rpc_failures":        STATE.rpc_failures,
            "retries":             STATE.retries,
            "chunks_ok":           STATE.chunks_ok,
            "chunks_failed":       STATE.chunks_failed,
            "peak_rss_bytes":      STATE.peak_rss_bytes,
            "artifact_disk_bytes": disk_used,
        },
        "failed_tasks": STATE.failed_tasks,
    }


def freeze_and_save_config(cfg: dict, pools: dict, blocks: dict) -> None:
    """Hash all three input manifests and write a snapshot for reproducibility."""
    snap = {
        "experiment":    "cohort_8w",
        "created_at":    datetime.now(timezone.utc).isoformat(),
        "config_sha256": sha256_bytes(json.dumps(cfg,    sort_keys=True, separators=(",", ":")).encode()),
        "pools_sha256":  sha256_bytes(json.dumps(pools,  sort_keys=True, separators=(",", ":")).encode()),
        "blocks_sha256": sha256_bytes(json.dumps(blocks, sort_keys=True, separators=(",", ":")).encode()),
        "environment": {
            "python":   platform.python_version(),
            "platform": platform.platform(),
            "machine":  platform.machine(),
        },
    }
    write_json_atomic(FROZEN_CFG_FILE, snap)
    log.info(f"Frozen config -> {FROZEN_CFG_FILE}")


def main() -> None:
    started_monotonic = time.monotonic()

    for d in (MANIFEST_DIR, RAW_DIR, PARQUET_DIR, RESULT_DIR):
        d.mkdir(parents=True, exist_ok=True)

    log.info("Loading manifests...")
    cfg, pools_manifest, blocks_manifest = load_configs()
    freeze_and_save_config(cfg, pools_manifest, blocks_manifest)

    token_map = build_token_map(cfg)
    log.info(f"Token map: {len(token_map)} entries")

    tasks = build_task_plan(cfg, pools_manifest, blocks_manifest)
    log.info(f"Task plan: {len(tasks)} tasks")

    log.info("Connecting to RPC endpoints...")
    w3_pool = build_web3_pool(cfg)

    checkpoint_data = load_checkpoint()
    log_startup_status(tasks, checkpoint_data)

    done_keys = {
        k for k, v in checkpoint_data.get("chunks", {}).items()
        if v.get("status") == "success" and v.get("fully_complete")
    }

    for i, task in enumerate(tasks, 1):
        if _task_ck_key(task) in done_keys:
            log.info(f"[{i}/{len(tasks)}] SKIP (complete): {task.chain_name}/{task.query_name}")
            continue

        log.info(
            f"[{i}/{len(tasks)}] RUNNING: chain={task.chain_name} | "
            f"type={task.record_type} | family={task.bridge_family or 'N/A'} | "
            f"query={task.query_name}"
        )

        w3 = w3_pool[task.chain_name]
        try:
            process_task(task, w3, token_map)
            checkpoint_task_complete(task)
            log.info(f"[{i}/{len(tasks)}] DONE: {task.chain_name}/{task.query_name}")
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            log.error(f"[{i}/{len(tasks)}] FAILED: {task.chain_name}/{task.query_name} -- {err}")
            checkpoint_task_failed(task, err)
            STATE.failed_tasks.append(f"{task.chain_name}:{task.query_name}:{err}")
            append_runtime({
                "event": "task_failed", "chain": task.chain_name, "query": task.query_name,
                "error_type": type(exc).__name__, "error": repr(exc),
                "traceback": traceback.format_exc(),
            })
            continue   # data integrity: never fabricate; skip and continue

    summary = build_summary(started_monotonic)
    write_json_atomic(SUMMARY_FILE, summary)

    log.info("=" * 70)
    if STATE.failed_tasks:
        log.warning(f"Completed with {len(STATE.failed_tasks)} failed task(s):")
        for ft in STATE.failed_tasks:
            log.warning(f"  FAILED: {ft}")
    else:
        log.info("All tasks completed successfully.")
    log.info(
        f"  swap_rows={STATE.swap_rows:,}  bridge_rows={STATE.bridge_rows:,}"
        f"  peak_rss={STATE.peak_rss_bytes / 1024**2:.1f} MB"
        f"  elapsed={time.monotonic() - started_monotonic:.1f}s"
    )
    log.info(f"  Summary -> {SUMMARY_FILE}")
    log.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        update_peak_rss()
        append_runtime({
            "event": "fatal_error", "error_type": type(exc).__name__,
            "error": repr(exc), "traceback": traceback.format_exc(),
            "peak_rss_bytes": STATE.peak_rss_bytes,
        })
        log.critical(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)