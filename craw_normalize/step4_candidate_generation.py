from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("candidate_gen_8w")

# Paths
ARTIFACT_DIR     = Path("artifact")
MANIFEST_DIR     = ARTIFACT_DIR / "manifests"
PARQUET_DIR      = ARTIFACT_DIR / "parquet"
DUCKDB_DIR       = ARTIFACT_DIR / "duckdb"
RESULT_DIR       = ARTIFACT_DIR / "results"

DB_FILE          = DUCKDB_DIR / "xmev.duckdb"
CONFIG_FILE      = MANIFEST_DIR / "mvp_config.json"
BLOCKS_FILE      = MANIFEST_DIR / "block_range_8w.json"
POOLS_FILE       = MANIFEST_DIR / "locked_pools_24.json"
CHECKPOINT_FILE  = RESULT_DIR / "step4_candidate_checkpoint.json"
SUMMARY_FILE     = RESULT_DIR / "step4_candidate_summary.json"
CANDIDATES_DIR   = PARQUET_DIR / "candidates"
TMP_DIR          = DUCKDB_DIR / "tmp"

# Chains mapping
CHAIN_NAMES = {
    1: "ethereum",
    42161: "arbitrum",
    8453: "base",
}
CHAIN_IDS = list(CHAIN_NAMES.keys())

# Default fallback latency buffer in seconds (30 minutes)
FALLBACK_LATENCY_BUFFER_SEC = 1800


@dataclass
class CandidateGenMetrics:
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    wall_time_seconds: float = 0.0
    peak_rss_bytes: int = 0
    total_candidates: int = 0
    total_swaps: int = 0
    batches_total: int = 48
    batches_completed: int = 0
    batches_skipped: int = 0
    candidates_by_pair: dict[str, int] = field(default_factory=dict)


def get_peak_rss() -> int:
    return psutil.Process(os.getpid()).memory_info().rss


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def load_checkpoint() -> dict[str, Any]:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"created_at": datetime.now(timezone.utc).isoformat(), "batches": {}}


def configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    conn.execute("SET threads = 2;")
    conn.execute("SET memory_limit = '4GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def setup_pool_mapping(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a normalized pool metadata table with exact token decimals and pairs."""
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE pool_canonical AS
        SELECT
            p.chain_id,
            LOWER(p.pool_address) AS pool_addr,
            CASE 
                WHEN t0.symbol < t1.symbol THEN t0.symbol || '/' || t1.symbol
                ELSE t1.symbol || '/' || t0.symbol
            END AS canonical_pair,
            t0.symbol AS t0_sym,
            t1.symbol AS t1_sym,
            CASE WHEN t0.symbol IN ('USDC', 'USDT') THEN 6 ELSE 18 END AS t0_dec,
            CASE WHEN t1.symbol IN ('USDC', 'USDT') THEN 6 ELSE 18 END AS t1_dec
        FROM metadata_pools p
        JOIN metadata_tokens t0 
          ON LOWER(p.token0_address) = LOWER(t0.address) AND p.chain_id = t0.chain_id
        JOIN metadata_tokens t1 
          ON LOWER(p.token1_address) = LOWER(t1.address) AND p.chain_id = t1.chain_id;
        """
    )


def compute_conditional_latencies(conn: duckdb.DuckDBPyConnection, quantile: float) -> dict[tuple[int, int, str], float]:
    """Compute empirical latency quantiles for each directed pair and bridge family."""
    rows = conn.execute(
        """
        SELECT
            src_chain,
            dst_chain,
            bridge_family,
            quantile_cont(receive_time - send_time, ?) AS lat_limit
        FROM bridge
        WHERE receive_time >= send_time
        GROUP BY src_chain, dst_chain, bridge_family;
        """,
        [quantile],
    ).fetchall()

    lat_map: dict[tuple[int, int, str], float] = {}
    for src, dst, fam, lat in rows:
        lat_map[(int(src), int(dst), str(fam))] = float(lat)
        log.info(f"Conditional latency {CHAIN_NAMES.get(src, src)} -> {CHAIN_NAMES.get(dst, dst)} ({fam}): {lat:.1f}s")
    return lat_map


def get_week_windows(cohort_start: int, cohort_end: int) -> list[tuple[int, int, int]]:
    """Generate 8 week boundaries (week_index, start_ts, end_ts)."""
    weeks = []
    sec_per_week = 7 * 86400
    for w in range(1, 9):
        w_start = cohort_start + (w - 1) * sec_per_week
        w_end = min(cohort_start + w * sec_per_week, cohort_end)
        weeks.append((w, w_start, w_end))
    return weeks


def process_batch(
    conn: duckdb.DuckDBPyConnection,
    src_chain: int,
    dst_chain: int,
    week_idx: int,
    w_start: int,
    w_end: int,
    amount_tolerance: float,
    lat_map: dict[tuple[int, int, str], float],
) -> int:
    """Execute high-speed candidate search for one directed chain-pair in one week."""
    # Latency limits for across and fallback
    lat_across = lat_map.get((src_chain, dst_chain, "across"), float(FALLBACK_LATENCY_BUFFER_SEC))
    lat_cctp = float(FALLBACK_LATENCY_BUFFER_SEC)  # Circle CCTP standard arrival buffer

    out_dir = CANDIDATES_DIR / f"src_chain={src_chain}" / f"dst_chain={dst_chain}" / f"week={week_idx}"
    out_file = out_dir / "part-0.parquet"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Source Leg (s1) filtered on src_chain and current week
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE batch_s1 AS
        SELECT
            s.chain_id AS src_chain,
            s.timestamp AS s1_timestamp,
            s.block_number AS s1_block,
            s.tx_hash AS s1_tx_hash,
            s.log_index AS s1_log_index,
            s.pool AS s1_pool,
            s.asset_out AS bridge_asset,
            s.amount_out AS s1_amount_out,
            pm.canonical_pair,
            CASE WHEN s.asset_out = pm.t0_sym THEN pm.t1_sym ELSE pm.t0_sym END AS s1_asset_in,
            CASE 
                WHEN CAST(s.amount0 AS DOUBLE) > 0 THEN CAST(s.amount0 AS DOUBLE) / POWER(10, pm.t0_dec)
                ELSE CAST(s.amount1 AS DOUBLE) / POWER(10, pm.t1_dec)
            END AS s1_amount_in
        FROM swaps s
        JOIN pool_canonical pm ON s.chain_id = pm.chain_id AND LOWER(s.pool) = pm.pool_addr
        WHERE s.chain_id = {src_chain}
          AND s.timestamp BETWEEN {w_start} AND {w_end}
          AND s.asset_out IN ('USDC', 'WETH');
        """
    )

    s1_count = conn.execute("SELECT COUNT(*) FROM batch_s1;").fetchone()[0]
    if s1_count == 0:
        return 0

    # 2. Bridge transfers matching src_chain, dst_chain, and week range
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE batch_bridge AS
        SELECT
            b.bridge_family,
            b.message_id,
            b.src_chain,
            b.dst_chain,
            b.asset_class,
            COALESCE(b.amount_received, b.amount_sent) AS bridge_amount,
            b.send_time,
            b.receive_time,
            COALESCE(b.receive_time, b.send_time) AS dst_arrival_time,
            b.send_tx,
            b.receive_tx,
            CASE WHEN b.bridge_family = 'across' THEN {lat_across} ELSE {lat_cctp} END AS allowed_latency
        FROM bridge b
        WHERE b.src_chain = {src_chain}
          AND b.dst_chain = {dst_chain}
          AND b.send_time BETWEEN {w_start} AND {w_end + FALLBACK_LATENCY_BUFFER_SEC}
          AND b.asset_class IN ('USDC', 'WETH');
        """
    )

    # 3. Match Leg 1 with Bridge
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE batch_s1_bridge AS
        SELECT
            s1.*,
            b.bridge_family,
            b.message_id,
            b.bridge_amount,
            b.send_time AS bridge_send_time,
            b.receive_time AS bridge_receive_time,
            b.dst_arrival_time,
            b.send_tx AS bridge_send_tx,
            b.receive_tx AS bridge_receive_tx,
            ABS(s1.s1_amount_out - b.bridge_amount) / NULLIF(s1.s1_amount_out, 0) AS leg1_bridge_mismatch
        FROM batch_s1 s1
        JOIN batch_bridge b
          ON s1.bridge_asset = b.asset_class
         AND b.send_time >= s1.s1_timestamp
         AND b.send_time <= s1.s1_timestamp + b.allowed_latency
        WHERE ABS(s1.s1_amount_out - b.bridge_amount) / NULLIF(s1.s1_amount_out, 0) <= {amount_tolerance};
        """
    )

    s1_b_count = conn.execute("SELECT COUNT(*) FROM batch_s1_bridge;").fetchone()[0]
    if s1_b_count == 0:
        return 0

    # Determine arrival timestamp window on destination chain
    min_arr, max_arr = conn.execute(
        "SELECT MIN(dst_arrival_time), MAX(dst_arrival_time) FROM batch_s1_bridge;"
    ).fetchone()

    # 4. Destination Leg (s2) filtered on dst_chain in the arrival window
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE batch_s2 AS
        SELECT
            s.chain_id AS dst_chain,
            s.timestamp AS s2_timestamp,
            s.block_number AS s2_block,
            s.tx_hash AS s2_tx_hash,
            s.log_index AS s2_log_index,
            s.pool AS s2_pool,
            s.asset_out AS s2_asset_out,
            s.amount_out AS s2_amount_out,
            pm.canonical_pair,
            CASE WHEN s.asset_out = pm.t0_sym THEN pm.t1_sym ELSE pm.t0_sym END AS s2_asset_in,
            CASE 
                WHEN CAST(s.amount0 AS DOUBLE) > 0 THEN CAST(s.amount0 AS DOUBLE) / POWER(10, pm.t0_dec)
                ELSE CAST(s.amount1 AS DOUBLE) / POWER(10, pm.t1_dec)
            END AS s2_amount_in
        FROM swaps s
        JOIN pool_canonical pm ON s.chain_id = pm.chain_id AND LOWER(s.pool) = pm.pool_addr
        WHERE s.chain_id = {dst_chain}
          AND s.timestamp BETWEEN {min_arr} AND {max_arr + FALLBACK_LATENCY_BUFFER_SEC};
        """
    )

    # 5. Join s1_bridge with s2, deduplicate by (s1_tx_hash, s1_log_index, message_id, s2_tx_hash, s2_log_index)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE batch_candidates AS
        WITH matched AS (
            SELECT
                -- Identifiers
                sb.src_chain,
                s2.dst_chain,
                sb.bridge_family,
                sb.canonical_pair,
                sb.bridge_asset,
                sb.s1_asset_in AS origin_asset,
                
                -- Leg 1
                sb.s1_block,
                sb.s1_timestamp,
                sb.s1_tx_hash,
                sb.s1_log_index,
                sb.s1_pool,
                sb.s1_amount_in,
                sb.s1_amount_out,

                -- Bridge
                sb.message_id,
                sb.bridge_amount,
                sb.bridge_send_time,
                sb.bridge_receive_time,
                sb.bridge_send_tx,
                sb.bridge_receive_tx,

                -- Leg 2
                s2.s2_block,
                s2.s2_timestamp,
                s2.s2_tx_hash,
                s2.s2_log_index,
                s2.s2_pool,
                s2.s2_amount_in,
                s2.s2_amount_out,

                -- Metrics
                sb.leg1_bridge_mismatch,
                ABS(s2.s2_amount_in - sb.bridge_amount) / NULLIF(sb.bridge_amount, 0) AS bridge_leg2_mismatch,
                (s2.s2_timestamp - sb.s1_timestamp) AS total_cycle_latency_sec,
                (s2.s2_amount_out - sb.s1_amount_in) AS gross_arbitrage_gain,

                ROW_NUMBER() OVER (
                    PARTITION BY sb.s1_tx_hash, sb.s1_log_index, sb.message_id, s2.s2_tx_hash, s2.s2_log_index
                    ORDER BY (s2.s2_timestamp - sb.s1_timestamp) ASC,
                             ABS(s2.s2_amount_in - sb.bridge_amount) ASC
                ) AS rn
            FROM batch_s1_bridge sb
            JOIN batch_s2 s2
              ON s2.s2_asset_in = sb.bridge_asset
             AND s2.s2_asset_out = sb.s1_asset_in
             AND s2.canonical_pair = sb.canonical_pair
             AND s2.s2_timestamp >= sb.dst_arrival_time
             AND s2.s2_timestamp <= sb.dst_arrival_time + {FALLBACK_LATENCY_BUFFER_SEC}
            WHERE ABS(s2.s2_amount_in - sb.bridge_amount) / NULLIF(sb.bridge_amount, 0) <= {amount_tolerance}
        )
        SELECT * EXCLUDE (rn)
        FROM matched
        WHERE rn = 1;
        """
    )

    candidate_count = conn.execute("SELECT COUNT(*) FROM batch_candidates;").fetchone()[0]

    if candidate_count > 0:
        tmp_parquet = out_file.with_suffix(".parquet.tmp")
        conn.execute(
            f"""
            COPY batch_candidates TO '{tmp_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        if out_file.exists():
            out_file.unlink()
        tmp_parquet.replace(out_file)

    return candidate_count


def create_unified_candidate_view(conn: duckdb.DuckDBPyConnection) -> int:
    """Create a unified canonical View over all generated candidate partitions."""
    candidate_files = list(CANDIDATES_DIR.rglob("*.parquet"))
    if not candidate_files:
        log.warning("No candidate parquet files found.")
        return 0

    conn.execute(
        """
        CREATE OR REPLACE VIEW candidates AS
        SELECT *
        FROM read_parquet(
            'artifact/parquet/candidates/*/*/*/*.parquet',
            hive_partitioning = true,
            union_by_name = true
        );
        """
    )
    total = conn.execute("SELECT COUNT(*) FROM candidates;").fetchone()[0]
    log.info(f"Unified View [candidates] created with {total:,} total records.")
    return total


def main() -> None:
    t0 = time.time()
    metrics = CandidateGenMetrics()
    DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== STEP 4: CANDIDATE GENERATION (RECALL-ORIENTED 2-LEG ARBITRAGE) ===")

    # 1. Load Configurations
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config manifest missing: {CONFIG_FILE}")
    mvp_cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    thresholds = mvp_cfg.get("candidate_generation_thresholds", {})
    amount_tolerance = float(thresholds.get("amount_tolerance", 0.01))
    latency_quantile = float(thresholds.get("latency_quantile", 0.95))

    blocks_cfg = json.loads(BLOCKS_FILE.read_text(encoding="utf-8"))
    c_start = int(blocks_cfg["block_resolution"]["chains"]["ethereum"]["start"]["timestamp"])
    c_end = int(blocks_cfg["block_resolution"]["chains"]["ethereum"]["end"]["timestamp"])

    checkpoint = load_checkpoint()

    # 2. Connect DuckDB
    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)

    try:
        # Step A: Setup Pool Decimals and Pair Mappings
        setup_pool_mapping(conn)

        # Step B: Empirical Conditional Latency (0.95 quantile)
        lat_map = compute_conditional_latencies(conn, latency_quantile)

        # Step C: Generate Week Time Windows
        weeks = get_week_windows(c_start, c_end)

        # Step D: Directed Pairs (6 pairs)
        directed_pairs = [
            (src, dst)
            for src in CHAIN_IDS
            for dst in CHAIN_IDS
            if src != dst
        ]

        total_batches = len(directed_pairs) * len(weeks)
        metrics.batches_total = total_batches
        log.info(f"Total Batches to process: {total_batches} (6 directed pairs x 8 weeks)")

        batch_counter = 0
        total_candidates_found = 0

        for src, dst in directed_pairs:
            pair_key = f"{CHAIN_NAMES[src]}_to_{CHAIN_NAMES[dst]}"
            metrics.candidates_by_pair.setdefault(pair_key, 0)

            for w_idx, w_start, w_end in weeks:
                batch_counter += 1
                batch_key = f"pair_{src}_{dst}_week_{w_idx}"

                # Checkpoint inspection
                if batch_key in checkpoint.get("batches", {}) and checkpoint["batches"][batch_key].get("status") == "completed":
                    cached_cnt = checkpoint["batches"][batch_key].get("candidates", 0)
                    total_candidates_found += cached_cnt
                    metrics.candidates_by_pair[pair_key] += cached_cnt
                    metrics.batches_skipped += 1
                    continue

                t_batch = time.time()
                cnt = process_batch(
                    conn,
                    src,
                    dst,
                    w_idx,
                    w_start,
                    w_end,
                    amount_tolerance,
                    lat_map,
                )
                b_dur = time.time() - t_batch
                total_candidates_found += cnt
                metrics.candidates_by_pair[pair_key] += cnt
                metrics.batches_completed += 1
                metrics.peak_rss_bytes = max(metrics.peak_rss_bytes, get_peak_rss())

                # Atomic Checkpoint Update
                checkpoint["batches"][batch_key] = {
                    "status": "completed",
                    "src_chain": src,
                    "dst_chain": dst,
                    "week": w_idx,
                    "candidates": cnt,
                    "duration_seconds": b_dur,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
                write_json_atomic(CHECKPOINT_FILE, checkpoint)

                log.info(
                    f"[{batch_counter}/{total_batches}] {CHAIN_NAMES[src].upper()} -> {CHAIN_NAMES[dst].upper()} "
                    f"Week {w_idx}: {cnt:,} candidates ({b_dur:.2f}s)"
                )

        # Step E: Create Unified View & Read Total Swaps
        total_swaps = conn.execute("SELECT COUNT(*) FROM swaps;").fetchone()[0]
        metrics.total_swaps = total_swaps
        metrics.total_candidates = create_unified_candidate_view(conn)
        metrics.wall_time_seconds = time.time() - t0
        metrics.peak_rss_bytes = max(metrics.peak_rss_bytes, get_peak_rss())

        rate_per_million = (
            (metrics.total_candidates / total_swaps * 1_000_000)
            if total_swaps > 0 else 0.0
        )

        # Step F: Final Summary Report
        summary = {
            "experiment": "cohort_8w",
            "status": "completed",
            "database_file": str(DB_FILE),
            "wall_time_seconds": metrics.wall_time_seconds,
            "peak_rss_bytes": metrics.peak_rss_bytes,
            "thresholds": {
                "amount_tolerance": amount_tolerance,
                "latency_quantile": latency_quantile,
                "fallback_latency_buffer_sec": FALLBACK_LATENCY_BUFFER_SEC,
            },
            "batches": {
                "total": metrics.batches_total,
                "completed": metrics.batches_completed,
                "skipped": metrics.batches_skipped,
            },
            "candidate_rate": {
                "total_candidates": metrics.total_candidates,
                "total_swaps": metrics.total_swaps,
                "candidates_per_million_swaps": rate_per_million,
            },
            "candidates_by_pair": metrics.candidates_by_pair,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(SUMMARY_FILE, summary)

        log.info(f"=== STEP 4 COMPLETED IN {metrics.wall_time_seconds:.2f}s ===")
        log.info(f"Total Candidates generated: {metrics.total_candidates:,}")
        log.info(f"Candidates per Million Swaps: {rate_per_million:.2f}")
        log.info(f"Peak RSS Memory: {metrics.peak_rss_bytes / (1024 * 1024):.1f} MB")
        log.info(f"Summary written to: {SUMMARY_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
