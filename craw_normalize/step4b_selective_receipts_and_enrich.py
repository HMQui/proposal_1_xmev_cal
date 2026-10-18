from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
import psutil
from dotenv import load_dotenv
from web3 import Web3
from web3.exceptions import TransactionNotFound

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("step4b_receipts_enrich")

# Public archive RPC fallback map (Section 3.1.5 & Section 11 of Guide)
FALLBACK_PUBLIC_RPCS = {
    1: "https://ethereum-rpc.publicnode.com",
    42161: "https://arb1.arbitrum.io/rpc",
    8453: "https://mainnet.base.org",
}

# Directories and Paths
ARTIFACT_DIR      = Path("artifact")
MANIFEST_DIR      = ARTIFACT_DIR / "manifests"
PARQUET_DIR       = ARTIFACT_DIR / "parquet"
DUCKDB_DIR        = ARTIFACT_DIR / "duckdb"
TRACES_DIR        = ARTIFACT_DIR / "traces"
RESULT_DIR        = ARTIFACT_DIR / "results"

DB_FILE           = DUCKDB_DIR / "xmev.duckdb"
CFG_8W_FILE       = MANIFEST_DIR / "8w_config.json"
QUEUE_FILE        = TRACES_DIR / "selective_tx_queue_2000.parquet"
CACHE_JSONL       = TRACES_DIR / "receipts_cache.jsonl"
RECEIPTS_PARQUET  = TRACES_DIR / "selective_receipts.parquet"
ENRICHED_PARQUET  = PARQUET_DIR / "candidates_enriched.parquet"
SUMMARY_FILE      = RESULT_DIR / "step4b_receipts_enrich_summary.json"
TMP_DIR           = DUCKDB_DIR / "tmp"

# Guide Section 1.1 & 5.5: Hard cap of 2,000 transactions for selective receipt/trace
TRACE_BUDGET_MAX_TX = 2000
MAX_RPC_WORKERS     = 2  # Guide Section 11: max 2 request workers
MAX_RETRIES         = 4


def get_peak_rss() -> int:
    proc = psutil.Process(os.getpid())
    mem_info = proc.memory_info()
    if hasattr(mem_info, "peak_wset"):
        return int(mem_info.peak_wset)
    return int(mem_info.rss)


def compute_file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def mask_endpoint(url: str) -> str:
    if not url:
        return "missing_endpoint"
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return "custom_rpc"


def configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    conn.execute("SET threads = 1;")
    conn.execute("SET memory_limit = '3GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def extract_candidate_swap_actors(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Memory-safe extraction of sender/beneficiary from [swaps] ONLY for (chain_id, tx_hash, log_index)
    present in [candidates]. Avoids full-table materialization and prevents row fan-out.
    """
    log.info("1. Extracting log-level sender/beneficiary for candidate legs from [swaps]...")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE needed_swap_keys AS
        SELECT DISTINCT src_chain AS chain_id, LOWER(s1_tx_hash) AS tx_hash, s1_log_index AS log_index FROM candidates
        UNION
        SELECT DISTINCT dst_chain AS chain_id, LOWER(s2_tx_hash) AS tx_hash, s2_log_index AS log_index FROM candidates;

        CREATE OR REPLACE TEMP TABLE candidate_swap_meta AS
        SELECT
            s.chain_id,
            LOWER(s.tx_hash) AS tx_hash,
            s.log_index,
            ANY_VALUE(LOWER(COALESCE(s.actor_eoa, s.sender))) AS log_sender,
            ANY_VALUE(LOWER(COALESCE(s.beneficiary, s.recipient))) AS log_beneficiary,
            ANY_VALUE(COALESCE(s.data_quality, 'ok')) AS swap_data_quality
        FROM swaps s
        INNER JOIN needed_swap_keys k
          ON s.chain_id = k.chain_id
         AND LOWER(s.tx_hash) = k.tx_hash
         AND s.log_index = k.log_index
        GROUP BY s.chain_id, LOWER(s.tx_hash), s.log_index;
        """
    )
    cnt = conn.execute("SELECT COUNT(*) FROM candidate_swap_meta;").fetchone()[0]
    log.info(f"   Matched {cnt:,} unique swap legs in candidate_swap_meta.")


def build_priority_queue_2000(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """
    Build deterministic Priority Queue capped at <= 2,000 unique (chain_id, tx_hash) per Guide Section 1.1 & 5.5:
      - Priority 1: All candidates with transport provenance link (LOWER(s1_tx_hash) = LOWER(bridge_send_tx)
                    or LOWER(s2_tx_hash) = LOWER(bridge_receive_tx)). (1,112 unique txs)
      - Priority 2: Unlinked candidates where log_beneficiary or log_sender matches across Leg 1 & Leg 2
                    and gross_arbitrage_gain > 0 (top Tier B / boundary hard-negative candidates).
      - Priority 3: Remaining closest boundary candidates (gross_arbitrage_gain > 0, smallest total mismatch).
    """
    log.info(f"2. Building Selective Receipt Priority Queue (cap = {TRACE_BUDGET_MAX_TX} unique txs)...")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranked_candidate_txs AS
        WITH cand_scored AS (
            SELECT
                c.src_chain,
                LOWER(c.s1_tx_hash) AS s1_tx,
                c.dst_chain,
                LOWER(c.s2_tx_hash) AS s2_tx,
                (
                    (c.bridge_send_tx IS NOT NULL AND LOWER(c.s1_tx_hash) = LOWER(c.bridge_send_tx))
                    OR (c.bridge_receive_tx IS NOT NULL AND LOWER(c.s2_tx_hash) = LOWER(c.bridge_receive_tx))
                ) AS is_provenance,
                (
                    m1.log_beneficiary = m2.log_beneficiary
                    OR m1.log_sender = m2.log_sender
                ) AS is_same_entity,
                (c.gross_arbitrage_gain > 0) AS is_pos_gross,
                (COALESCE(c.leg1_bridge_mismatch, 1.0) + COALESCE(c.bridge_leg2_mismatch, 1.0)) AS total_mismatch,
                c.gross_arbitrage_gain,
                CASE
                    WHEN (c.bridge_send_tx IS NOT NULL AND LOWER(c.s1_tx_hash) = LOWER(c.bridge_send_tx))
                      OR (c.bridge_receive_tx IS NOT NULL AND LOWER(c.s2_tx_hash) = LOWER(c.bridge_receive_tx))
                    THEN 1
                    WHEN (m1.log_beneficiary = m2.log_beneficiary OR m1.log_sender = m2.log_sender)
                     AND c.gross_arbitrage_gain > 0
                    THEN 2
                    WHEN c.gross_arbitrage_gain > 0
                    THEN 3
                    ELSE 4
                END AS priority_tier
            FROM candidates c
            LEFT JOIN candidate_swap_meta m1
              ON c.src_chain = m1.chain_id AND LOWER(c.s1_tx_hash) = m1.tx_hash AND c.s1_log_index = m1.log_index
            LEFT JOIN candidate_swap_meta m2
              ON c.dst_chain = m2.chain_id AND LOWER(c.s2_tx_hash) = m2.tx_hash AND c.s2_log_index = m2.log_index
        ),
        unpivoted AS (
            SELECT src_chain AS chain_id, s1_tx AS tx_hash, priority_tier, total_mismatch, gross_arbitrage_gain FROM cand_scored
            UNION ALL
            SELECT dst_chain AS chain_id, s2_tx AS tx_hash, priority_tier, total_mismatch, gross_arbitrage_gain FROM cand_scored
        ),
        deduped AS (
            SELECT
                chain_id,
                tx_hash,
                MIN(priority_tier) AS priority_tier,
                MIN(total_mismatch) AS best_mismatch,
                MAX(gross_arbitrage_gain) AS max_gross_gain
            FROM unpivoted
            GROUP BY chain_id, tx_hash
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY priority_tier ASC, best_mismatch ASC, max_gross_gain DESC, chain_id ASC, tx_hash ASC
            ) AS queue_rank,
            chain_id,
            tx_hash,
            priority_tier,
            best_mismatch,
            max_gross_gain
        FROM deduped
        QUALIFY queue_rank <= {TRACE_BUDGET_MAX_TX};
        """
    )

    TRACES_DIR.mkdir(parents=True, exist_ok=True)
    conn.execute(
        f"""
        COPY (SELECT * FROM ranked_candidate_txs ORDER BY queue_rank ASC)
        TO '{QUEUE_FILE.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    rows = conn.execute(
        "SELECT queue_rank, chain_id, tx_hash, priority_tier FROM ranked_candidate_txs ORDER BY queue_rank ASC;"
    ).fetchall()

    tier_Breakdown = conn.execute(
        "SELECT priority_tier, COUNT(*) FROM ranked_candidate_txs GROUP BY priority_tier ORDER BY priority_tier;"
    ).fetchall()
    log.info(f"   Selected {len(rows):,} unique transactions for receipt queue. Breakdown by tier: {dict(tier_Breakdown)}")

    return [
        {
            "queue_rank": int(r[0]),
            "chain_id": int(r[1]),
            "tx_hash": str(r[2]),
            "priority_tier": int(r[3]),
        }
        for r in rows
    ]


def parse_hex_or_int(val: Any) -> int:
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        val = val.strip()
        return int(val, 16) if val.startswith("0x") else int(val)
    return int(val)


def _extract_receipt_dict(
    rcpt: Any,
    w3_primary: Web3,
    item: dict[str, Any],
    endpoint_masked: str,
    attempt: int,
) -> dict[str, Any]:
    chain_id = item["chain_id"]
    tx_hash = item["tx_hash"]
    priority_tier = item["priority_tier"]
    queue_rank = item["queue_rank"]

    tx_from = str(rcpt.get("from", "")).lower() or None
    tx_to = str(rcpt.get("to", "")).lower() if rcpt.get("to") else None
    gas_used = parse_hex_or_int(rcpt.get("gasUsed", 0))
    eff_price = parse_hex_or_int(rcpt.get("effectiveGasPrice", 0))
    l1_fee = parse_hex_or_int(rcpt.get("l1Fee", 0))
    tx_status_int = parse_hex_or_int(rcpt.get("status", 1))

    # Fallback if RPC omits effectiveGasPrice in receipt
    if eff_price == 0 and gas_used > 0:
        try:
            tx_obj = w3_primary.eth.get_transaction(tx_hash)
            if tx_obj:
                eff_price = parse_hex_or_int(tx_obj.get("gasPrice", 0))
        except Exception:
            pass

    total_fee_wei = (gas_used * eff_price) + l1_fee
    gas_native = float(total_fee_wei) / 1e18

    return {
        "queue_rank": queue_rank,
        "chain_id": chain_id,
        "tx_hash": tx_hash,
        "priority_tier": priority_tier,
        "status": "ok" if tx_status_int == 1 else "reverted",
        "tx_status_int": tx_status_int,
        "actor_eoa": tx_from,
        "first_called_contract": tx_to,
        "gas_used": gas_used,
        "effective_gas_price_wei": eff_price,
        "l1_fee_wei": l1_fee,
        "gas_native": gas_native,
        "endpoint": endpoint_masked,
        "attempt_count": attempt,
        "error_code": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_single_receipt(
    w3: Web3,
    fallback_w3: Web3 | None,
    endpoint_masked: str,
    item: dict[str, Any],
) -> dict[str, Any]:
    chain_id = item["chain_id"]
    tx_hash = item["tx_hash"]
    priority_tier = item["priority_tier"]
    queue_rank = item["queue_rank"]

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rcpt = w3.eth.get_transaction_receipt(tx_hash)
            if rcpt is None:
                raise TransactionNotFound(f"Transaction receipt not found on primary endpoint: {tx_hash}")

            return _extract_receipt_dict(rcpt, w3, item, endpoint_masked, attempt)

        except TransactionNotFound:
            # Try once with fallback public archive RPC without sleep retries
            if fallback_w3 is not None:
                try:
                    fb_rcpt = fallback_w3.eth.get_transaction_receipt(tx_hash)
                    if fb_rcpt is not None:
                        return _extract_receipt_dict(
                            fb_rcpt,
                            fallback_w3,
                            item,
                            mask_endpoint(FALLBACK_PUBLIC_RPCS.get(chain_id, "")),
                            attempt,
                        )
                except Exception:
                    pass

            # Both primary and fallback reported not found / None
            return {
                "queue_rank": queue_rank,
                "chain_id": chain_id,
                "tx_hash": tx_hash,
                "priority_tier": priority_tier,
                "status": "missing",
                "tx_status_int": -1,
                "actor_eoa": None,
                "first_called_contract": None,
                "gas_used": None,
                "effective_gas_price_wei": None,
                "l1_fee_wei": 0,
                "gas_native": None,
                "endpoint": endpoint_masked,
                "attempt_count": attempt,
                "error_code": "receipt_not_found",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:120]}"
            if attempt < MAX_RETRIES:
                sleep_s = (0.6 * (2 ** (attempt - 1))) + random.uniform(0.05, 0.25)
                time.sleep(sleep_s)

    return {
        "queue_rank": queue_rank,
        "chain_id": chain_id,
        "tx_hash": tx_hash,
        "priority_tier": priority_tier,
        "status": "rpc_error",
        "tx_status_int": -1,
        "actor_eoa": None,
        "first_called_contract": None,
        "gas_used": None,
        "effective_gas_price_wei": None,
        "l1_fee_wei": 0,
        "gas_native": None,
        "endpoint": endpoint_masked,
        "attempt_count": MAX_RETRIES,
        "error_code": last_err,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_receipts_with_checkpoint(queue_items: list[dict[str, Any]]) -> None:
    load_dotenv()
    cfg_8w = json.loads(CFG_8W_FILE.read_text(encoding="utf-8"))

    w3_map: dict[int, tuple[Web3, Web3 | None, str]] = {}
    for _, chain_cfg in cfg_8w.get("chains", {}).items():
        cid = int(chain_cfg["chain_id"])
        rpc_env = str(chain_cfg["rpc_env"])
        rpc_url = os.getenv(rpc_env, "").strip()
        if not rpc_url:
            raise RuntimeError(f"Environment variable {rpc_env} is missing or empty in .env")
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))

        fallback_url = FALLBACK_PUBLIC_RPCS.get(cid)
        fallback_w3 = Web3(Web3.HTTPProvider(fallback_url, request_kwargs={"timeout": 30})) if fallback_url else None

        w3_map[cid] = (w3, fallback_w3, mask_endpoint(rpc_url))

    # Load existing checkpoint cache
    cached_records: dict[tuple[int, str], dict[str, Any]] = {}
    if CACHE_JSONL.exists():
        with open(CACHE_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    # Only keep successful 'ok' or 'reverted' records in cache; retry 'rpc_error'
                    if rec.get("status") in ("ok", "reverted"):
                        cached_records[(int(rec["chain_id"]), str(rec["tx_hash"]).lower())] = rec
                except Exception:
                    continue

    pending = [
        item for item in queue_items
        if (item["chain_id"], item["tx_hash"].lower()) not in cached_records
    ]
    log.info(
        f"3. Fetching Selective Receipts: {len(cached_records):,} already cached, "
        f"{len(pending):,} remaining to fetch (max workers = {MAX_RPC_WORKERS})..."
    )

    if pending:
        TRACES_DIR.mkdir(parents=True, exist_ok=True)
        completed_now = 0
        with open(CACHE_JSONL, "a", encoding="utf-8") as cache_f:
            with ThreadPoolExecutor(max_workers=MAX_RPC_WORKERS) as executor:
                future_to_item = {
                    executor.submit(
                        fetch_single_receipt,
                        w3_map[item["chain_id"]][0],
                        w3_map[item["chain_id"]][1],
                        w3_map[item["chain_id"]][2],
                        item,
                    ): item
                    for item in pending
                }
                for fut in as_completed(future_to_item):
                    res = fut.result()
                    cached_records[(res["chain_id"], res["tx_hash"].lower())] = res
                    cache_f.write(json.dumps(res, ensure_ascii=False) + "\n")
                    completed_now += 1
                    if completed_now % 100 == 0 or completed_now == len(pending):
                        cache_f.flush()
                        log.info(f"   Progress: {completed_now:,}/{len(pending):,} receipts fetched...")

    # Write deterministic JSONL in exact queue order before converting to Parquet via DuckDB
    ordered_jsonl = TRACES_DIR / "receipts_ordered.tmp.jsonl"
    with open(ordered_jsonl, "w", encoding="utf-8") as out_f:
        for item in queue_items:
            key = (item["chain_id"], item["tx_hash"].lower())
            rec = cached_records.get(key)
            if rec is not None:
                rec["queue_rank"] = item["queue_rank"]
                rec["priority_tier"] = item["priority_tier"]
                out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)
    try:
        tmp_pq = RECEIPTS_PARQUET.with_suffix(".parquet.tmp")
        conn.execute(
            f"""
            COPY (
                SELECT
                    CAST(queue_rank AS BIGINT) AS queue_rank,
                    CAST(chain_id AS BIGINT) AS chain_id,
                    LOWER(CAST(tx_hash AS VARCHAR)) AS tx_hash,
                    CAST(priority_tier AS INTEGER) AS priority_tier,
                    CAST(status AS VARCHAR) AS status,
                    CAST(tx_status_int AS INTEGER) AS tx_status_int,
                    LOWER(CAST(actor_eoa AS VARCHAR)) AS actor_eoa,
                    LOWER(CAST(first_called_contract AS VARCHAR)) AS first_called_contract,
                    CAST(gas_used AS BIGINT) AS gas_used,
                    CAST(effective_gas_price_wei AS BIGINT) AS effective_gas_price_wei,
                    CAST(l1_fee_wei AS BIGINT) AS l1_fee_wei,
                    CAST(gas_native AS DOUBLE) AS gas_native,
                    CAST(endpoint AS VARCHAR) AS endpoint,
                    CAST(attempt_count AS INTEGER) AS attempt_count,
                    CAST(error_code AS VARCHAR) AS error_code,
                    CAST(fetched_at AS VARCHAR) AS fetched_at
                FROM read_json_auto('{ordered_jsonl.as_posix()}')
                ORDER BY queue_rank ASC
            )
            TO '{tmp_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        if RECEIPTS_PARQUET.exists():
            RECEIPTS_PARQUET.unlink()
        tmp_pq.replace(RECEIPTS_PARQUET)
        ordered_jsonl.unlink(missing_ok=True)

        conn.execute(
            f"""
            CREATE OR REPLACE VIEW selective_receipts AS
            SELECT * FROM read_parquet('{RECEIPTS_PARQUET.as_posix()}');
            """
        )
    finally:
        conn.close()


def build_enriched_candidates(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Join [candidates] with [candidate_swap_meta], [selective_receipts], and [prices]
    to produce enriched candidate records with full actor/contract/beneficiary and net profit after gas.
    """
    log.info("4. Enriching [candidates] with actor metadata, gas_native, and net_arbitrage_gain...")
    tmp_enriched = ENRICHED_PARQUET.with_suffix(".parquet.tmp")

    conn.execute(
        f"""
        COPY (
            WITH deduped_prices AS (
                SELECT
                    asset_class,
                    hour_ts,
                    ANY_VALUE(price_usd) AS price_usd
                FROM prices
                GROUP BY asset_class, hour_ts
            ),
            base_joined AS (
                SELECT
                    c.*,
                    -- Leg 1 Actor & Contract Metadata
                    COALESCE(r1.actor_eoa, m1.log_sender) AS s1_actor_eoa,
                    r1.first_called_contract AS s1_first_called_contract,
                    m1.log_sender AS s1_swap_sender,
                    m1.log_beneficiary AS s1_beneficiary,
                    COALESCE(r1.status, 'not_in_selective_budget') AS s1_receipt_status,
                    r1.gas_native AS s1_gas_native,

                    -- Leg 2 Actor & Contract Metadata
                    COALESCE(r2.actor_eoa, m2.log_sender) AS s2_actor_eoa,
                    r2.first_called_contract AS s2_first_called_contract,
                    m2.log_sender AS s2_swap_sender,
                    m2.log_beneficiary AS s2_beneficiary,
                    COALESCE(r2.status, 'not_in_selective_budget') AS s2_receipt_status,
                    r2.gas_native AS s2_gas_native,

                    -- Hourly USD prices at Leg 1 and Leg 2 timestamps
                    p_eth1.price_usd AS s1_eth_usd_price,
                    p_eth2.price_usd AS s2_eth_usd_price,
                    p_orig.price_usd AS origin_asset_usd_price
                FROM candidates c
                LEFT JOIN candidate_swap_meta m1
                  ON c.src_chain = m1.chain_id AND LOWER(c.s1_tx_hash) = m1.tx_hash AND c.s1_log_index = m1.log_index
                LEFT JOIN candidate_swap_meta m2
                  ON c.dst_chain = m2.chain_id AND LOWER(c.s2_tx_hash) = m2.tx_hash AND c.s2_log_index = m2.log_index
                LEFT JOIN selective_receipts r1
                  ON c.src_chain = r1.chain_id AND LOWER(c.s1_tx_hash) = r1.tx_hash
                LEFT JOIN selective_receipts r2
                  ON c.dst_chain = r2.chain_id AND LOWER(c.s2_tx_hash) = r2.tx_hash
                LEFT JOIN deduped_prices p_eth1
                  ON p_eth1.asset_class = 'ETH' AND p_eth1.hour_ts = (c.s1_timestamp // 3600) * 3600
                LEFT JOIN deduped_prices p_eth2
                  ON p_eth2.asset_class = 'ETH' AND p_eth2.hour_ts = (c.s2_timestamp // 3600) * 3600
                LEFT JOIN deduped_prices p_orig
                  ON p_orig.asset_class = c.origin_asset AND p_orig.hour_ts = (c.s1_timestamp // 3600) * 3600
            ),
            computed AS (
                SELECT
                    *,
                    COALESCE(s1_gas_native * s1_eth_usd_price, 0.0) AS s1_gas_usd,
                    COALESCE(s2_gas_native * s2_eth_usd_price, 0.0) AS s2_gas_usd,
                    COALESCE(s1_gas_native * s1_eth_usd_price, 0.0) + COALESCE(s2_gas_native * s2_eth_usd_price, 0.0) AS total_gas_usd,
                    (
                        COALESCE(s1_gas_native * s1_eth_usd_price, 0.0) + COALESCE(s2_gas_native * s2_eth_usd_price, 0.0)
                    ) / NULLIF(origin_asset_usd_price, 0.0) AS total_gas_origin_asset,
                    CASE
                        WHEN s1_receipt_status = 'ok' AND s2_receipt_status = 'ok' THEN 'both_receipts_verified'
                        WHEN s1_receipt_status IN ('missing', 'rpc_error', 'reverted')
                          OR s2_receipt_status IN ('missing', 'rpc_error', 'reverted') THEN 'receipt_missing_or_failed'
                        WHEN s1_receipt_status = 'ok' OR s2_receipt_status = 'ok' THEN 'partial_receipt_verified'
                        ELSE 'log_only_untraced'
                    END AS receipt_data_quality
                FROM base_joined
            )
            SELECT
                *,
                gross_arbitrage_gain - COALESCE(total_gas_origin_asset, 0.0) AS net_arbitrage_gain,
                (gross_arbitrage_gain - COALESCE(total_gas_origin_asset, 0.0)) * origin_asset_usd_price AS net_arbitrage_gain_usd
            FROM computed
            ORDER BY src_chain, dst_chain, s1_timestamp, s1_tx_hash, s2_tx_hash
        )
        TO '{tmp_enriched.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    if ENRICHED_PARQUET.exists():
        ENRICHED_PARQUET.unlink()
    tmp_enriched.replace(ENRICHED_PARQUET)

    log.info("5. Updating canonical [candidates] View in DuckDB to point to candidates_enriched.parquet...")
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW candidates_raw AS
        SELECT *
        FROM read_parquet(
            'artifact/parquet/candidates/*/*/*/*.parquet',
            hive_partitioning = true,
            union_by_name = true
        );

        CREATE OR REPLACE VIEW candidates AS
        SELECT *
        FROM read_parquet('{ENRICHED_PARQUET.as_posix()}');
        """
    )

    # Row Conservation Assertion: must exactly equal 380,225
    raw_count = conn.execute("SELECT COUNT(*) FROM candidates_raw;").fetchone()[0]
    enriched_count = conn.execute("SELECT COUNT(*) FROM candidates;").fetchone()[0]
    log.info(f"Row count validation: candidates_raw = {raw_count:,} | candidates_enriched = {enriched_count:,}")
    if enriched_count != 380225 or enriched_count != raw_count:
        raise RuntimeError(
            f"Row Conservation Violation! Expected exactly 380,225 (matching raw {raw_count:,}), "
            f"but found {enriched_count:,} in candidates_enriched!"
        )


def main() -> None:
    t0 = time.time()
    TRACES_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== STEP 4B: SELECTIVE RECEIPT FETCHER (<= 2,000 TX) & CANDIDATE ENRICHMENT ===")

    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)
    try:
        extract_candidate_swap_actors(conn)
        queue_items = build_priority_queue_2000(conn)
    finally:
        conn.close()

    # Fetch Receipts via RPC outside DuckDB write lock
    fetch_receipts_with_checkpoint(queue_items)

    # Reconnect to DuckDB to build enriched candidates
    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)
    try:
        extract_candidate_swap_actors(conn)
        build_enriched_candidates(conn)

        rcpt_stats = conn.execute(
            """
            SELECT status, priority_tier, COUNT(*)
            FROM selective_receipts
            GROUP BY status, priority_tier
            ORDER BY priority_tier, status;
            """
        ).fetchall()

        qual_stats = conn.execute(
            """
            SELECT receipt_data_quality, COUNT(*)
            FROM candidates
            GROUP BY receipt_data_quality
            ORDER BY COUNT(*) DESC;
            """
        ).fetchall()

        prov_qual = conn.execute(
            """
            SELECT receipt_data_quality,
                   COUNT(*) AS total,
                   SUM(CASE WHEN gross_arbitrage_gain > 0 THEN 1 ELSE 0 END) AS pos_gross,
                   SUM(CASE WHEN net_arbitrage_gain > 0 THEN 1 ELSE 0 END) AS pos_net
            FROM candidates
            WHERE LOWER(s1_tx_hash) = LOWER(bridge_send_tx)
            GROUP BY receipt_data_quality;
            """
        ).fetchall()

        wall_time = round(time.time() - t0, 3)
        peak_rss = get_peak_rss()

        summary = {
            "experiment": "cohort_8w",
            "task": "step4b_selective_receipts_and_enrich",
            "status": "completed",
            "wall_time_seconds": wall_time,
            "peak_rss_bytes": peak_rss,
            "trace_budget_max_tx": TRACE_BUDGET_MAX_TX,
            "receipts_parquet": str(RECEIPTS_PARQUET),
            "receipts_parquet_sha256": compute_file_sha256(RECEIPTS_PARQUET),
            "enriched_candidates_parquet": str(ENRICHED_PARQUET),
            "enriched_candidates_sha256": compute_file_sha256(ENRICHED_PARQUET),
            "receipt_status_by_tier": [
                {"status": r[0], "priority_tier": int(r[1]), "count": int(r[2])}
                for r in rcpt_stats
            ],
            "candidate_receipt_quality": {str(r[0]): int(r[1]) for r in qual_stats},
            "atomic_leg1_provenance_net_vs_gross": [
                {
                    "receipt_data_quality": str(r[0]),
                    "total_candidates": int(r[1]),
                    "positive_gross_gain": int(r[2]),
                    "positive_net_gain_after_gas": int(r[3]),
                }
                for r in prov_qual
            ],
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(SUMMARY_FILE, summary)

        log.info(f"=== STEP 4B COMPLETED IN {wall_time:.2f}s ===")
        log.info(f"Receipt Status Breakdown: {summary['receipt_status_by_tier']}")
        log.info(f"Candidate Quality Breakdown: {summary['candidate_receipt_quality']}")
        log.info(f"Provenance Net vs Gross: {summary['atomic_leg1_provenance_net_vs_gross']}")
        log.info(f"Summary saved to: {SUMMARY_FILE}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()