from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
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
log = logging.getLogger("load_duckdb_8w")

ARTIFACT_DIR    = Path("artifact")
PARQUET_DIR     = ARTIFACT_DIR / "parquet"
DUCKDB_DIR      = ARTIFACT_DIR / "duckdb"
RESULT_DIR      = ARTIFACT_DIR / "results"
MANIFEST_DIR    = ARTIFACT_DIR / "manifests"

DB_FILE         = DUCKDB_DIR / "xmev.duckdb"
CHECKPOINT_FILE = RESULT_DIR / "step3_load_checkpoint.json"
SUMMARY_FILE    = RESULT_DIR / "step3_load_summary.json"
CONFIG_FILE     = MANIFEST_DIR / "8w_config.json"
POOLS_FILE      = MANIFEST_DIR / "locked_pools_24.json"
BLOCKS_FILE     = MANIFEST_DIR / "block_range_8w.json"

TMP_DIR         = DUCKDB_DIR / "tmp"
BATCH_SIZE      = 400  # Safe batch size to strictly stay below OS open file limits (ulimit -n)


@dataclass
class LoadMetrics:
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    wall_time_seconds: float = 0.0
    peak_rss_bytes: int = 0
    partitions_total: int = 0
    partitions_compacted: int = 0
    partitions_skipped: int = 0
    total_swap_rows: int = 0
    total_bridge_rows: int = 0
    quality_checks: dict[str, Any] = field(default_factory=dict)


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
    return {"created_at": datetime.now(timezone.utc).isoformat(), "partitions": {}}


def configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    conn.execute("SET threads = 2;")
    conn.execute("SET memory_limit = '4GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def discover_partitions(base_dir: Path) -> list[Path]:
    """Find all canonical partition directories, ignoring internal _raw_parts archives."""
    partitions = set()
    for p in base_dir.rglob("*.parquet"):
        if "_raw_parts" in p.parts:
            continue
        partitions.add(p.parent)
    return sorted(partitions)


def compact_partition(conn: duckdb.DuckDBPyConnection, part_dir: Path, checkpoint: dict[str, Any]) -> tuple[int, bool]:
    """Compact all part-*.parquet files in part_dir in safe batches into a single compacted.parquet."""
    part_key = part_dir.relative_to(ARTIFACT_DIR).as_posix()
    target_file = part_dir / "compacted.parquet"
    raw_parts_dir = part_dir / "_raw_parts"

    # 1. Check if already marked completed in checkpoint
    if part_key in checkpoint["partitions"] and checkpoint["partitions"][part_key].get("status") == "completed":
        if target_file.exists():
            rows = checkpoint["partitions"][part_key].get("rows", 0)
            log.info(f"Skipping already compacted partition [{part_key}] ({rows:,} rows).")
            return rows, False

    part_files = sorted(list(part_dir.glob("part-*.parquet")))
    if not part_files:
        if target_file.exists():
            cnt = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{target_file.as_posix()}')").fetchone()[0]
            checkpoint["partitions"][part_key] = {"status": "completed", "rows": cnt, "files_merged": 1}
            write_json_atomic(CHECKPOINT_FILE, checkpoint)
            log.info(f"Partition [{part_key}] already has compacted.parquet ({cnt:,} rows). Checkpointed.")
            return cnt, False
        return 0, False

    tmp_target = part_dir / "compacted.parquet.tmp"
    if tmp_target.exists():
        tmp_target.unlink()

    # 2. If single batch, compact directly; if multiple batches, use two-stage hierarchical compaction
    batches = [part_files[i : i + BATCH_SIZE] for i in range(0, len(part_files), BATCH_SIZE)]
    num_batches = len(batches)
    log.info(f"Compacting {len(part_files)} files in [{part_key}] across {num_batches} batch(es)...")

    # Clean up any leftover stage1 temp files from previous interrupted runs
    for leftover in part_dir.glob("_tmp_stage1_batch_*.parquet"):
        try:
            leftover.unlink()
        except OSError:
            pass

    if num_batches == 1:
        file_paths = [f.as_posix() for f in part_files]
        conn.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet(?, union_by_name = true)
            ) TO '{tmp_target.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """,
            [file_paths],
        )
    else:
        stage1_files = []
        try:
            for idx, batch in enumerate(batches, start=1):
                stage1_tmp = part_dir / f"_tmp_stage1_batch_{idx}.parquet"
                stage1_files.append(stage1_tmp)
                file_paths = [f.as_posix() for f in batch]
                conn.execute(
                    f"""
                    COPY (
                        SELECT * FROM read_parquet(?, union_by_name = true)
                    ) TO '{stage1_tmp.as_posix()}' (FORMAT PARQUET);
                    """,
                    [file_paths],
                )
                if idx % 5 == 0 or idx == num_batches:
                    log.info(f"  Stage 1: Processed batch {idx}/{num_batches} ({len(batch)} files)...")

            # Stage 2: Merge all intermediate batch files into final target
            log.info(f"  Stage 2: Merging {len(stage1_files)} batch files into final compacted target...")
            stage1_paths = [f.as_posix() for f in stage1_files]
            conn.execute(
                f"""
                COPY (
                    SELECT * FROM read_parquet(?, union_by_name = true)
                ) TO '{tmp_target.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """,
                [stage1_paths],
            )
        finally:
            # Clean up intermediate stage1 files
            for sf in stage1_files:
                if sf.exists():
                    try:
                        sf.unlink()
                    except OSError:
                        pass

    rows_compacted = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_target.as_posix()}')").fetchone()[0]

    # Verify & atomic rename
    tmp_target.replace(target_file)

    # Safely archive raw chunk files into _raw_parts
    raw_parts_dir.mkdir(parents=True, exist_ok=True)
    for pf in part_files:
        dest = raw_parts_dir / pf.name
        if dest.exists():
            dest.unlink()
        pf.replace(dest)

    checkpoint["partitions"][part_key] = {
        "status": "completed",
        "rows": rows_compacted,
        "files_merged": len(part_files),
        "compacted_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(CHECKPOINT_FILE, checkpoint)
    log.info(f"  -> Partition [{part_key}] done: {rows_compacted:,} rows merged.")
    return rows_compacted, True


def load_metadata_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create physical lookup tables for locked pools and token manifests."""
    log.info("Loading metadata tables into DuckDB...")
    
    if POOLS_FILE.exists():
        pools_data = json.loads(POOLS_FILE.read_text(encoding="utf-8"))
        pools_list = pools_data.get("pools", pools_data.get("selected_pools", []))
        conn.execute("DROP TABLE IF EXISTS metadata_pools;")
        conn.execute(
            """
            CREATE TABLE metadata_pools (
                chain_id BIGINT,
                chain_name VARCHAR,
                dex_family VARCHAR,
                pool_address VARCHAR,
                pair_name VARCHAR,
                fee_tier INTEGER,
                token0_symbol VARCHAR,
                token0_address VARCHAR,
                token0_decimals INTEGER,
                token1_symbol VARCHAR,
                token1_address VARCHAR,
                token1_decimals INTEGER
            );
            """
        )
        for p in pools_list:
            conn.execute(
                """
                INSERT INTO metadata_pools VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    p.get("chain_id"),
                    p.get("chain_name"),
                    p.get("dex_family"),
                    p.get("pool_address"),
                    p.get("pair_name"),
                    p.get("fee_tier"),
                    p.get("token0_symbol"),
                    p.get("token0_contract"),
                    p.get("token0_decimals"),
                    p.get("token1_symbol"),
                    p.get("token1_contract"),
                    p.get("token1_decimals"),
                ],
            )
        log.info(f"Loaded metadata_pools ({len(pools_list)} records).")

    if CONFIG_FILE.exists():
        cfg_data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        conn.execute("DROP TABLE IF EXISTS metadata_tokens;")
        conn.execute(
            """
            CREATE TABLE metadata_tokens (
                chain_name VARCHAR,
                chain_id BIGINT,
                symbol VARCHAR,
                address VARCHAR,
                wrapper_canonical_relation VARCHAR,
                verification_source VARCHAR
            );
            """
        )
        for cname, cinfo in cfg_data.get("chains", {}).items():
            cid = cinfo.get("chain_id")
            for sym, tinfo in cinfo.get("tokens", {}).items():
                conn.execute(
                    """
                    INSERT INTO metadata_tokens VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    [
                        cname,
                        cid,
                        sym,
                        tinfo.get("address"),
                        tinfo.get("wrapper_canonical_relation"),
                        tinfo.get("verification_source"),
                    ],
                )
        log.info("Loaded metadata_tokens.")


def create_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create standardized canonical Views for swaps and cross-chain bridge events."""
    log.info("Creating canonical Views...")

    # 1. SWAPS VIEW
    conn.execute(
        """
        CREATE OR REPLACE VIEW swaps AS
        SELECT
            chain_id,
            block_number,
            block_hash,
            timestamp,
            tx_hash,
            log_index,
            pool,
            dex_family,
            fee_tier,
            sender,
            recipient,
            actor_eoa,
            beneficiary,
            asset_out,
            amount_out,
            amount0,
            amount1,
            sqrt_price_x96,
            liquidity,
            tick,
            decoder_version,
            data_quality,
            year_month
        FROM read_parquet(
            'artifact/parquet/swaps/chain_id=*/year_month=*/*.parquet',
            hive_partitioning = true,
            union_by_name = true
        );
        """
    )

    # 2. ACROSS BRIDGE VIEW
    conn.execute(
        """
        CREATE OR REPLACE VIEW bridge_across_raw AS
        SELECT *
        FROM read_parquet(
            'artifact/parquet/bridge/bridge_family=across/year_month=*/*.parquet',
            hive_partitioning = true,
            union_by_name = true
        );

        CREATE OR REPLACE VIEW bridge_across AS
        SELECT
            'across' AS bridge_family,
            message_id,
            MAX(src_chain) AS src_chain,
            MAX(dst_chain) AS dst_chain,
            MAX(asset_class) AS asset_class,
            MAX(TRY_CAST(amount_sent AS DOUBLE)) AS amount_sent,
            MAX(TRY_CAST(amount_received AS DOUBLE)) AS amount_received,
            MAX(TRY_CAST(bridge_fee AS DOUBLE)) AS bridge_fee,
            MAX(send_time) AS send_time,
            MAX(receive_time) AS receive_time,
            MAX(send_tx) AS send_tx,
            MAX(receive_tx) AS receive_tx,
            'exact_event_log' AS provenance_type,
            'settled' AS status
        FROM bridge_across_raw
        GROUP BY message_id;
        """
    )

    # 3. CCTP BRIDGE VIEW
    # Map CCTP domains: 0 -> 1 (Ethereum), 3 -> 42161 (Arbitrum), 6 -> 8453 (Base)
    conn.execute(
        """
        CREATE OR REPLACE VIEW bridge_cctp_raw AS
        SELECT *
        FROM read_parquet(
            'artifact/parquet/bridge/bridge_family=cctp/*/*/*.parquet',
            hive_partitioning = true,
            union_by_name = true
        );

        CREATE OR REPLACE VIEW bridge_cctp AS
        SELECT
            'cctp' AS bridge_family,
            CAST(nonce AS VARCHAR) AS message_id,
            chain_id AS src_chain,
            CASE destination_domain
                WHEN 0 THEN 1
                WHEN 3 THEN 42161
                WHEN 6 THEN 8453
                ELSE destination_domain
            END AS dst_chain,
            'USDC' AS asset_class,
            TRY_CAST(amount_raw AS DOUBLE) / 1000000.0 AS amount_sent,
            TRY_CAST(amount_raw AS DOUBLE) / 1000000.0 AS amount_received,
            0.0 AS bridge_fee,
            timestamp AS send_time,
            CAST(NULL AS BIGINT) AS receive_time,
            tx_hash AS send_tx,
            CAST(NULL AS VARCHAR) AS receive_tx,
            'burn_event' AS provenance_type,
            'deposited' AS status
        FROM bridge_cctp_raw
        WHERE event_name = 'DepositForBurn';
        """
    )

    # 4. UNIFIED BRIDGE VIEW
    conn.execute(
        """
        CREATE OR REPLACE VIEW bridge AS
        SELECT * FROM bridge_across
        UNION ALL
        SELECT * FROM bridge_cctp;
        """
    )
    log.info("Canonical Views created: [swaps], [bridge_across], [bridge_cctp], [bridge].")


def run_quality_gates(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Execute integrity checks across datasets."""
    log.info("Running Data Quality Gates...")

    # Swaps metrics
    swaps_summary = conn.execute(
        """
        SELECT
            chain_id,
            year_month,
            COUNT(*) AS row_count,
            MIN(timestamp) AS min_ts,
            MAX(timestamp) AS max_ts,
            COUNT(CASE WHEN tx_hash IS NULL THEN 1 END) AS null_tx,
            COUNT(CASE WHEN pool IS NULL THEN 1 END) AS null_pool
        FROM swaps
        GROUP BY chain_id, year_month
        ORDER BY chain_id, year_month;
        """
    ).df().to_dict(orient="records")

    total_swaps = conn.execute("SELECT COUNT(*) FROM swaps;").fetchone()[0]

    # Bridge metrics
    bridge_summary = conn.execute(
        """
        SELECT
            bridge_family,
            COUNT(*) AS total_messages,
            COUNT(CASE WHEN receive_tx IS NOT NULL THEN 1 END) AS paired_messages,
            MIN(send_time) AS min_send_time,
            MAX(send_time) AS max_send_time
        FROM bridge
        GROUP BY bridge_family;
        """
    ).df().to_dict(orient="records")

    total_bridge = conn.execute("SELECT COUNT(*) FROM bridge;").fetchone()[0]

    return {
        "total_swaps": total_swaps,
        "swaps_by_partition": swaps_summary,
        "total_bridge_messages": total_bridge,
        "bridge_by_family": bridge_summary,
    }


def main() -> None:
    t0 = time.time()
    metrics = LoadMetrics()
    DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== STEP 3: DATA INGESTION & DUCKDB VERIFICATION ===")
    checkpoint = load_checkpoint()

    # Step A: Connect DuckDB & configure limits
    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)

    try:
        # Step B: Discover and Compact Partitions
        partitions = discover_partitions(PARQUET_DIR)
        metrics.partitions_total = len(partitions)
        log.info(f"Discovered {len(partitions)} canonical partition directories.")

        for part_dir in partitions:
            rows, compacted = compact_partition(conn, part_dir, checkpoint)
            if compacted:
                metrics.partitions_compacted += 1
            else:
                metrics.partitions_skipped += 1
            metrics.peak_rss_bytes = max(metrics.peak_rss_bytes, get_peak_rss())

        # Step C: Metadata Tables
        load_metadata_tables(conn)

        # Step D: Canonical Views
        create_views(conn)

        # Step E: Data Quality Gate
        q_results = run_quality_gates(conn)
        metrics.quality_checks = q_results
        metrics.total_swap_rows = q_results["total_swaps"]
        metrics.total_bridge_rows = q_results["total_bridge_messages"]

        metrics.wall_time_seconds = time.time() - t0
        metrics.peak_rss_bytes = max(metrics.peak_rss_bytes, get_peak_rss())

        # Summary
        summary = {
            "experiment": "cohort_8w",
            "status": "completed",
            "database_file": str(DB_FILE),
            "wall_time_seconds": metrics.wall_time_seconds,
            "peak_rss_bytes": metrics.peak_rss_bytes,
            "partitions": {
                "total": metrics.partitions_total,
                "compacted": metrics.partitions_compacted,
                "skipped": metrics.partitions_skipped,
            },
            "observed": {
                "total_swaps": metrics.total_swap_rows,
                "total_bridge_messages": metrics.total_bridge_rows,
            },
            "quality_checks": metrics.quality_checks,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(SUMMARY_FILE, summary)
        log.info(f"=== STEP 3 COMPLETED IN {metrics.wall_time_seconds:.2f}s ===")
        log.info(f"Total Swaps in DB: {metrics.total_swap_rows:,}")
        log.info(f"Total Bridge Messages: {metrics.total_bridge_rows:,}")
        log.info(f"Peak RSS Memory: {metrics.peak_rss_bytes / (1024 * 1024):.1f} MB")
        log.info(f"Summary written to {SUMMARY_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
