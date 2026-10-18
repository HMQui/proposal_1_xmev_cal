from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
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
log = logging.getLogger("step4a_build_prices")

# Directories and Paths
ARTIFACT_DIR = Path("artifact")
PARQUET_DIR  = ARTIFACT_DIR / "parquet"
DUCKDB_DIR   = ARTIFACT_DIR / "duckdb"
RESULT_DIR   = ARTIFACT_DIR / "results"
PRICES_DIR   = PARQUET_DIR / "prices"

DB_FILE      = DUCKDB_DIR / "xmev.duckdb"
SUMMARY_FILE = RESULT_DIR / "step4a_prices_summary.json"
TMP_DIR      = DUCKDB_DIR / "tmp"


BLOCKS_FILE  = ARTIFACT_DIR / "manifests" / "block_range_8w.json"


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


def configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    # Guide Section 2 & 11: strictly bounded threads and RAM for safe execution
    conn.execute("SET threads = 1;")
    conn.execute("SET memory_limit = '3GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def build_prices_tables(conn: duckdb.DuckDBPyConnection) -> None:
    log.info("1. Building pool_canonical metadata mapping...")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE pool_canonical AS
        SELECT
            p.chain_id,
            LOWER(p.pool_address) AS pool_addr,
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

    log.info("2. Extracting hourly WETH/USD observations from on-chain swaps (WETH vs USDC/USDT/DAI)...")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE weth_hourly_raw AS
        WITH parsed_swaps AS (
            SELECT
                s.chain_id,
                s.year_month,
                (s.timestamp // 3600) * 3600 AS hour_ts,
                pm.t0_sym,
                pm.t1_sym,
                ABS(CAST(s.amount0 AS DOUBLE)) / POWER(10, pm.t0_dec) AS amt0,
                ABS(CAST(s.amount1 AS DOUBLE)) / POWER(10, pm.t1_dec) AS amt1
            FROM swaps s
            JOIN pool_canonical pm
              ON s.chain_id = pm.chain_id AND LOWER(s.pool) = pm.pool_addr
            WHERE s.year_month IN ('2025-09', '2025-10')
              AND ((pm.t0_sym = 'WETH' AND pm.t1_sym IN ('USDC', 'USDT', 'DAI'))
               OR  (pm.t1_sym = 'WETH' AND pm.t0_sym IN ('USDC', 'USDT', 'DAI')))
        ),
        valid_obs AS (
            SELECT
                chain_id,
                year_month,
                hour_ts,
                CASE WHEN t0_sym = 'WETH' THEN amt0 ELSE amt1 END AS weth_amt,
                CASE WHEN t0_sym = 'WETH' THEN amt1 ELSE amt0 END AS usd_amt,
                (CASE WHEN t0_sym = 'WETH' THEN amt1 ELSE amt0 END) /
                NULLIF(CASE WHEN t0_sym = 'WETH' THEN amt0 ELSE amt1 END, 0) AS implied_weth_usd
            FROM parsed_swaps
            WHERE amt0 > 0 AND amt1 > 0
        )
        SELECT
            hour_ts,
            year_month,
            median(implied_weth_usd) AS weth_price_usd,
            SUM(usd_amt) / NULLIF(SUM(weth_amt), 0) AS weth_vwap_usd,
            COUNT(*) AS sample_swaps
        FROM valid_obs
        WHERE implied_weth_usd BETWEEN 500.0 AND 15000.0
        GROUP BY hour_ts, year_month;
        """
    )

    log.info("3. Building complete hourly grid across the 8-week cohort with forward/backward fill fallback...")
    if not BLOCKS_FILE.exists():
        raise FileNotFoundError(f"Block range manifest missing: {BLOCKS_FILE}")
    blocks_cfg = json.loads(BLOCKS_FILE.read_text(encoding="utf-8"))
    cohort_start = int(blocks_cfg["block_resolution"]["chains"]["ethereum"]["start"]["timestamp"])
    cohort_end = int(blocks_cfg["block_resolution"]["chains"]["ethereum"]["end"]["timestamp"])
    min_hour = (cohort_start // 3600) * 3600
    max_hour = (cohort_end // 3600) * 3600

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE cohort_bounds AS
        SELECT
            {min_hour} AS min_hour,
            {max_hour} AS max_hour,
            median(weth_price_usd) AS global_median_weth
        FROM weth_hourly_raw;

        CREATE OR REPLACE TEMP TABLE complete_weth_hourly AS
        WITH hour_grid AS (
            SELECT
                UNNEST(generate_series(
                    {min_hour},
                    {max_hour},
                    3600
                )) AS hour_ts
        ),
        joined AS (
            SELECT
                g.hour_ts,
                strftime(to_timestamp(g.hour_ts), '%Y-%m') AS year_month,
                w.weth_price_usd,
                w.weth_vwap_usd,
                COALESCE(w.sample_swaps, 0) AS sample_swaps
            FROM hour_grid g
            LEFT JOIN weth_hourly_raw w ON g.hour_ts = w.hour_ts
        ),
        filled AS (
            SELECT
                hour_ts,
                year_month,
                COALESCE(
                    weth_price_usd,
                    LAST_VALUE(weth_price_usd IGNORE NULLS) OVER (ORDER BY hour_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                    FIRST_VALUE(weth_price_usd IGNORE NULLS) OVER (ORDER BY hour_ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                    (SELECT global_median_weth FROM cohort_bounds)
                ) AS price_usd,
                COALESCE(
                    weth_vwap_usd,
                    weth_price_usd,
                    (SELECT global_median_weth FROM cohort_bounds)
                ) AS vwap_usd,
                sample_swaps,
                CASE WHEN weth_price_usd IS NOT NULL THEN 'observed_onchain_median' ELSE 'interpolated_nearest_hour' END AS price_source
            FROM joined
        )
        SELECT * FROM filled;
        """
    )

    log.info("4. Building unified [all_prices] table for WETH, ETH, USDC, USDT, DAI...")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE all_prices AS
        -- WETH
        SELECT
            'WETH' AS asset_class,
            year_month,
            hour_ts,
            price_usd,
            vwap_usd,
            sample_swaps,
            price_source
        FROM complete_weth_hourly
        UNION ALL
        -- ETH (1:1 canonical equivalence with WETH for native gas valuation)
        SELECT
            'ETH' AS asset_class,
            year_month,
            hour_ts,
            price_usd,
            vwap_usd,
            sample_swaps,
            price_source
        FROM complete_weth_hourly
        UNION ALL
        -- Stablecoins (USDC, USDT, DAI anchored at 1.0 USD with on-chain swap parity)
        SELECT
            asset_c AS asset_class,
            w.year_month,
            w.hour_ts,
            1.0::DOUBLE AS price_usd,
            1.0::DOUBLE AS vwap_usd,
            w.sample_swaps,
            'canonical_usd_unit' AS price_source
        FROM complete_weth_hourly w
        CROSS JOIN (VALUES ('USDC'), ('USDT'), ('DAI')) AS t(asset_c);
        """
    )


def export_partitioned_prices(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log.info("5. Exporting partitioned Parquet files to artifact/parquet/prices/...")
    partitions = conn.execute(
        """
        SELECT DISTINCT asset_class, year_month
        FROM all_prices
        ORDER BY asset_class, year_month;
        """
    ).fetchall()

    file_manifests: dict[str, Any] = {}
    for asset_class, ym in partitions:
        out_dir = PRICES_DIR / f"asset_class={asset_class}" / f"year_month={ym}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "part-0.parquet"
        tmp_file = out_file.with_suffix(".parquet.tmp")

        conn.execute(
            f"""
            COPY (
                SELECT
                    hour_ts,
                    price_usd,
                    vwap_usd,
                    sample_swaps,
                    price_source
                FROM all_prices
                WHERE asset_class = '{asset_class}' AND year_month = '{ym}'
                ORDER BY hour_ts ASC
            )
            TO '{tmp_file.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        if out_file.exists():
            out_file.unlink()
        tmp_file.replace(out_file)

        rel_key = f"asset_class={asset_class}/year_month={ym}/part-0.parquet"
        file_manifests[rel_key] = {
            "sha256": compute_file_sha256(out_file),
            "size_bytes": out_file.stat().st_size,
        }

    log.info("6. Registering canonical [prices] View in DuckDB...")
    conn.execute(
        """
        CREATE OR REPLACE VIEW prices AS
        SELECT *
        FROM read_parquet(
            'artifact/parquet/prices/asset_class=*/year_month=*/*.parquet',
            hive_partitioning = true
        );
        """
    )
    return file_manifests


def main() -> None:
    t0 = time.time()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== STEP 4A: BUILD ON-CHAIN HOURLY PRICES & REGISTER DUCKDB VIEW ===")
    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)

    try:
        build_prices_tables(conn)
        file_manifests = export_partitioned_prices(conn)

        # Collect verification stats from the newly registered [prices] view
        total_rows = conn.execute("SELECT COUNT(*) FROM prices;").fetchone()[0]
        weth_stats = conn.execute(
            """
            SELECT
                COUNT(*) AS hours_cnt,
                MIN(price_usd) AS min_price,
                median(price_usd) AS median_price,
                MAX(price_usd) AS max_price,
                SUM(CASE WHEN price_source = 'observed_onchain_median' THEN 1 ELSE 0 END) AS observed_hours
            FROM prices
            WHERE asset_class = 'WETH';
            """
        ).fetchone()

        wall_time = round(time.time() - t0, 3)
        peak_rss = get_peak_rss()

        summary = {
            "experiment": "cohort_8w",
            "task": "step4a_build_prices",
            "status": "completed",
            "wall_time_seconds": wall_time,
            "peak_rss_bytes": peak_rss,
            "total_price_rows": int(total_rows),
            "weth_price_summary": {
                "total_hours": int(weth_stats[0]),
                "observed_onchain_hours": int(weth_stats[4]),
                "min_weth_usd": round(float(weth_stats[1]), 4),
                "median_weth_usd": round(float(weth_stats[2]), 4),
                "max_weth_usd": round(float(weth_stats[3]), 4),
            },
            "partitions": file_manifests,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(SUMMARY_FILE, summary)

        log.info(f"=== STEP 4A COMPLETED IN {wall_time:.2f}s ===")
        log.info(f"Total rows in [prices] view: {total_rows:,}")
        log.info(
            f"WETH USD Price (Min / Median / Max): "
            f"${weth_stats[1]:,.2f} / ${weth_stats[2]:,.2f} / ${weth_stats[3]:,.2f} "
            f"({weth_stats[4]}/{weth_stats[0]} hours directly observed)"
        )
        log.info(f"Summary saved to: {SUMMARY_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()