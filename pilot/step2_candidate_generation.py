import json
import argparse
from pathlib import Path

from database import get_db_connection
from datetime import datetime

CONFIG_FILE = Path("artifact/manifests/pilot_24h_config.json")
SUMMARY_FILE = Path("artifact/results/pilot_24h_summary.json")

DEFAULT_CONFIG = Path("artifact/manifests/pilot_24h_config.json")

def get_config_path():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
    )
    args = parser.parse_args()
    return Path(args.config)

CONFIG_FILE = get_config_path()

SWAPS_GLOB = (
    "artifact/parquet/swaps/chain_id=*/year_month=*/*.parquet"
)
BRIDGE_GLOB = (
    "artifact/parquet/bridge/"
    "bridge_family=*/year_month=*/*.parquet"
)

def load_config() -> dict:
    """Load the frozen experiment configuration."""
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_parameters(config: dict) -> dict:
    """Read only preregistered candidate-generation parameters."""
    thresholds = config["candidate_generation_thresholds"]

    # Đọc timestamp từ pilot config
    eth_block_res = config["block_resolution"]["chains"]["ethereum"]
    cohort_start = eth_block_res["start"]["timestamp"]
    cohort_end = eth_block_res["end"]["timestamp"]

    # Đọc danh sách chain và DỊCH sang chain_id (dạng chuỗi để so sánh SQL)
    chain_names = config.get("chains", config.get("chain_pair", []))
    chain_ids = []
    for c in chain_names:
        c_id = config["block_resolution"]["chains"][c]["chain_id"]
        chain_ids.append(str(c_id))

    dex_protocols = ["uniswap_v3"]
    asset_classes = ["USDC", "WETH"]
    bridge_families = [config["bridge"]["family"]] if "bridge" in config else ["across"]

    return {
        "cohort_start": cohort_start,
        "cohort_end": cohort_end,
        "chains": chain_ids,
        "dex_protocols": dex_protocols,
        "asset_classes": asset_classes,
        "bridge_families": bridge_families,
        "max_depth": config.get("max_depth", 2),
        "amount_tolerance": float(thresholds["amount_tolerance"]),
        "latency_quantile": float(thresholds["latency_quantile"]),
    }


def configure_connection(conn) -> None:
    """Enforce the documented DuckDB resource limits."""
    conn.execute("SET threads = 1")
    conn.execute("SET memory_limit = '4GB'")
    conn.execute(
        "SET temp_directory = 'artifact/duckdb/tmp'"
    )
    conn.execute(
        "SET preserve_insertion_order = false"
    )


def create_views(conn) -> None:
    """Create lazy views over partitioned Parquet and merge cross-chain bridge events."""
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW swaps AS
        SELECT *
        FROM read_parquet(
            '{SWAPS_GLOB}',
            hive_partitioning = true,
            union_by_name = true
        )
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE VIEW bridge_raw AS
        SELECT *
        FROM read_parquet(
            '{BRIDGE_GLOB}',
            hive_partitioning = true,
            union_by_name = true
        )
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE VIEW bridge AS
        SELECT
            bridge_family,
            message_id,
            MAX(src_chain) AS src_chain,
            MAX(dst_chain) AS dst_chain,
            MAX(asset_class) AS asset_class,
            MAX(amount_sent) AS amount_sent,
            MAX(amount_received) AS amount_received,
            MAX(send_time) AS send_time,
            MAX(receive_time) AS receive_time,
            MAX(send_tx) AS send_tx,
            MAX(receive_tx) AS receive_tx
        FROM bridge_raw
        GROUP BY
            bridge_family,
            message_id
        """
    )


def validate_schema(conn) -> None:
    """Validate the canonical fields used by candidate generation."""
    required = {
        "swaps": {
            "chain_id",
            "timestamp",
            "tx_hash",
            "log_index",
            "pool",
            "dex_family",
            "asset_in",
            "asset_out",
            "amount_in",
            "amount_out",
        },
        "bridge": {
            "bridge_family",
            "message_id",
            "src_chain",
            "dst_chain",
            "asset_class",
            "amount_sent",
            "send_time",
            "receive_time",
        },
    }

    for table, expected in required.items():
        rows = conn.execute(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            """
        ).fetchall()

        actual = {row[0] for row in rows}
        missing = expected - actual

        if missing:
            raise RuntimeError(
                f"{table}: missing required fields: "
                f"{sorted(missing)}"
            )


def create_conditional_latency(
    conn,
    parameters: dict,
) -> None:
    """Estimate empirical latency by directed chain pair and bridge family."""
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE conditional_latency AS
        SELECT
            src_chain,
            dst_chain,
            bridge_family,
            quantile_cont(
                receive_time - send_time,
                ?
            ) AS latency_limit
        FROM bridge
        WHERE receive_time >= send_time
          AND src_chain <> dst_chain
          AND CAST(src_chain AS VARCHAR)
              IN (
                  SELECT UNNEST(?)
              )
          AND CAST(dst_chain AS VARCHAR)
              IN (
                  SELECT UNNEST(?)
              )
          AND CAST(bridge_family AS VARCHAR)
              IN (
                  SELECT UNNEST(?)
              )
        GROUP BY
            src_chain,
            dst_chain,
            bridge_family
        """,
        [
            parameters["latency_quantile"],
            parameters["chains"],
            parameters["chains"],
            parameters["bridge_families"],
        ],
    )


def create_exhaustive_pairs(
    conn,
    parameters: dict,
) -> None:
    """Build the broad recall-oriented inverse-asset candidate universe."""
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE exhaustive_pairs AS
        SELECT
            s.chain_id AS src_chain,
            b.dst_chain,
            b.bridge_family,

            s.tx_hash AS swap_tx_hash,
            s.log_index AS swap_log_index,
            b.message_id,

            s.timestamp AS swap_time,
            b.send_time,
            b.receive_time,

            s.asset_out,
            b.asset_class,

            TRY_CAST(
                s.amount_out AS DOUBLE
            ) AS swap_amount,

            COALESCE(
                TRY_CAST(b.amount_sent AS DOUBLE),
                TRY_CAST(b.amount_received AS DOUBLE)
            ) AS bridge_amount,

            ABS(
                COALESCE(
                    TRY_CAST(b.amount_sent AS DOUBLE),
                    TRY_CAST(b.amount_received AS DOUBLE)
                )
                -
                TRY_CAST(s.amount_out AS DOUBLE)
            )
            /
            NULLIF(
                ABS(
                    TRY_CAST(
                        s.amount_out AS DOUBLE
                    )
                ),
                0
            ) AS amount_mismatch

        FROM swaps s

        JOIN bridge b
          ON s.chain_id = b.src_chain
         AND s.chain_id <> b.dst_chain

         AND s.asset_out = b.asset_class

         AND b.send_time >= s.timestamp

        WHERE
            s.timestamp >= ?
            AND s.timestamp <= ?
            AND b.send_time >= ?
            AND b.send_time <= ?

            AND CAST(s.chain_id AS VARCHAR)
                IN (
                    SELECT UNNEST(?)
                )

            AND CAST(b.dst_chain AS VARCHAR)
                IN (
                    SELECT UNNEST(?)
                )

            AND CAST(s.dex_family AS VARCHAR)
                IN (
                    SELECT UNNEST(?)
                )

            AND CAST(s.asset_out AS VARCHAR)
                IN (
                    SELECT UNNEST(?)
                )

            AND CAST(b.asset_class AS VARCHAR)
                IN (
                    SELECT UNNEST(?)
                )

            AND CAST(b.bridge_family AS VARCHAR)
                IN (
                    SELECT UNNEST(?)
                )
        """,
        [
            parameters["cohort_start"],
            parameters["cohort_end"],
            parameters["cohort_start"],
            parameters["cohort_end"],
            parameters["chains"],
            parameters["chains"],
            parameters["dex_protocols"],
            parameters["asset_classes"],
            parameters["asset_classes"],
            parameters["bridge_families"],
        ],
    )


def create_thresholded_candidates(
    conn,
    tolerance: float,
) -> None:
    """Apply the preregistered amount bound and empirical latency cutoff."""
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE candidates_raw AS
        SELECT
            ep.*,
            cl.latency_limit,

            ep.receive_time - ep.send_time
                AS bridge_delay

        FROM exhaustive_pairs ep

        JOIN conditional_latency cl
          ON ep.src_chain = cl.src_chain
         AND ep.dst_chain = cl.dst_chain
         AND ep.bridge_family = cl.bridge_family

        WHERE
            ep.amount_mismatch <= ?

            AND (
                ep.receive_time - ep.send_time
            ) <= cl.latency_limit
        """,
        [tolerance],
    )


def deduplicate_candidates(conn) -> None:
    """Deduplicate event-level candidates before model input."""
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE candidates AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        swap_tx_hash,
                        swap_log_index,
                        message_id
                    ORDER BY
                        bridge_delay ASC,
                        amount_mismatch ASC
                ) AS rn
            FROM candidates_raw
        )
        WHERE rn = 1
        """
    )


def count_rows(conn, table: str) -> int:
    """Count rows inside DuckDB without materializing them."""
    return conn.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]


def calculate_pruning_loss(
    exhaustive_count: int,
    candidate_count: int,
) -> float:
    """Measure loss against the exhaustive 24-hour candidate universe."""
    if exhaustive_count == 0:
        return 0.0

    return max(
        0.0,
        1.0
        - (
            candidate_count
            / exhaustive_count
        ),
    )


def update_summary(
    candidates: int,
    exhaustive_count: int,
    pruning_loss: float,
    parameters: dict,
) -> None:
    """Persist reproducible candidate-generation metrics."""
    with SUMMARY_FILE.open("r", encoding="utf-8") as f:
        summary = json.load(f)

    swaps = int(
        summary["observed"]["swap_rows"]
    )

    rate = (
        candidates / swaps * 1_000_000
        if swaps > 0
        else 0.0
    )

    summary["status"] = "completed"

    summary["candidate_rate"] = {
        "status": "computed",
        "candidates": candidates,
        "swaps": swaps,
        "candidates_per_million_swaps": rate,
        "exhaustive_candidates": exhaustive_count,
        "pruning_loss": pruning_loss,
        "amount_tolerance": parameters[
            "amount_tolerance"
        ],
        "latency_quantile": parameters[
            "latency_quantile"
        ],
        "cohort_start": parameters[
            "cohort_start"
        ],
        "cohort_end": parameters[
            "cohort_end"
        ],
    }

    with SUMMARY_FILE.open("w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
            ensure_ascii=False,
        )


def main() -> None:
    conn = get_db_connection()

    try:
        config = load_config()
        parameters = get_parameters(config)

        configure_connection(conn)
        create_views(conn)
        validate_schema(conn)

        create_conditional_latency(
            conn,
            parameters,
        )

        create_exhaustive_pairs(
            conn,
            parameters,
        )

        exhaustive_count = count_rows(
            conn,
            "exhaustive_pairs",
        )

        create_thresholded_candidates(
            conn,
            parameters["amount_tolerance"],
        )

        deduplicate_candidates(conn)

        candidate_count = count_rows(
            conn,
            "candidates",
        )

        pruning_loss = calculate_pruning_loss(
            exhaustive_count,
            candidate_count,
        )

        update_summary(
            candidate_count,
            exhaustive_count,
            pruning_loss,
            parameters,
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
    