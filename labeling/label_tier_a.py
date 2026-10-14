from __future__ import annotations

import hashlib
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
log = logging.getLogger("tier_a_labeling")

# Directories and Paths
ARTIFACT_DIR    = Path("artifact")
PARQUET_DIR     = ARTIFACT_DIR / "parquet"
DUCKDB_DIR      = ARTIFACT_DIR / "duckdb"
RESULT_DIR      = ARTIFACT_DIR / "results"
LABELS_DIR      = ARTIFACT_DIR / "labels"
MANIFEST_DIR    = ARTIFACT_DIR / "manifests"

DB_FILE         = DUCKDB_DIR / "xmev.duckdb"
CONFIG_FILE     = MANIFEST_DIR / "mvp_config.json"
LABEL_FILE      = LABELS_DIR / "tier_a_labels.parquet"
SUMMARY_FILE    = RESULT_DIR / "step5_tier_a_labeling_summary.json"
TMP_DIR         = DUCKDB_DIR / "tmp"

# Leg 2 Flow Reconciliation Threshold for Discrete Relayer Fill Transactions (10 bps = 0.001)
# Ensures that discrete Leg 2 swaps are strictly reconciled to bridge receive amount if not in same tx.
LEG2_TIGHT_FLOW_TOLERANCE = 0.001


@dataclass
class TierALabelingMetrics:
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    wall_time_seconds: float = 0.0
    peak_rss_bytes: int = 0
    total_candidates: int = 0
    positives: int = 0
    natural_hard_negatives: int = 0
    unclear: int = 0
    by_rationale: dict[str, int] = field(default_factory=dict)
    by_provenance_type: dict[str, int] = field(default_factory=dict)
    by_bridge_family: dict[str, dict[str, int]] = field(default_factory=dict)
    by_chain_pair: dict[str, dict[str, int]] = field(default_factory=dict)
    label_file_sha256: str = ""


def get_peak_rss() -> int:
    """Return true peak RSS / working set in bytes across Windows, Linux, and macOS."""
    proc = psutil.Process(os.getpid())
    mem_info = proc.memory_info()
    if hasattr(mem_info, "peak_wset"):
        return int(mem_info.peak_wset)
    try:
        import resource
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        if sys.platform == "darwin":
            return int(rusage.ru_maxrss)
        return int(rusage.ru_maxrss * 1024)
    except Exception:
        return int(mem_info.rss)


def compute_file_sha256(path: Path) -> str:
    """Calculate SHA-256 checksum of an artifact file."""
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
    # Guide Section 2 & 11: 8 GB RAM laptop strictly specifies threads = 1, memory_limit = '4GB'.
    # Single-thread execution also ensures strict deterministic parquet ordering and reproducible SHA-256.
    conn.execute("SET threads = 1;")
    conn.execute("SET memory_limit = '4GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def ensure_candidates_view(conn: duckdb.DuckDBPyConnection) -> bool:
    """Ensure that the candidates table/view exists and has rows."""
    try:
        cnt = conn.execute("SELECT COUNT(*) FROM candidates;").fetchone()[0]
        log.info(f"Connected to existing [candidates] view with {cnt:,} records.")
        return cnt > 0
    except Exception:
        candidate_parquets = list(PARQUET_DIR.glob("candidates/*/*/*/*.parquet"))
        if not candidate_parquets:
            log.error("No candidate parquet files found in artifact/parquet/candidates/.")
            return False
        log.info("Creating [candidates] view from parquet files...")
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
        cnt = conn.execute("SELECT COUNT(*) FROM candidates;").fetchone()[0]
        log.info(f"Created [candidates] view with {cnt:,} records.")
        return cnt > 0


def generate_tier_a_labels(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Apply Section 4.1 & Section 11 Tier A ground truth logic:
    
    1. Unclear / Missing Evidence (Section 4.2 & Section 11):
       - Missing source bridge evidence (bridge_send_time IS NULL, bridge_send_tx IS NULL, or invalid message_id).
       - Timestamp causality violation or clock skew:
         * s1_timestamp > bridge_send_time, OR
         * bridge_send_time > s2_timestamp, OR
         * total_cycle_latency_sec < 0, OR
         * (when bridge_receive_time is present): bridge_send_time > bridge_receive_time OR bridge_receive_time > s2_timestamp.
       - Invalid or non-positive token amounts.
       -> MUST be classified as 'unclear' (excluded from test truth).

    2. Tier A Positive (Section 4.1):
       - Exact provenance transport anchor:
         * Valid exact message_id / deterministic burn-mint nonce.
         * Atomic router / same-transaction link on Leg 1: LOWER(s1_tx_hash) = LOWER(bridge_send_tx).
         * Leg 2 Provenance link or tight flow reconciliation:
           LOWER(s2_tx_hash) = LOWER(bridge_receive_tx) OR bridge_leg2_mismatch <= LEG2_TIGHT_FLOW_TOLERANCE.
       - Closed 2-leg cycle with valid timeline across chains.
       - Observed value positive: gross_arbitrage_gain > 0 (net of protocol & bridge fees, pre-gas receipt).

    3. Natural Hard Negative (Section 4.1):
       - Valid candidate in universe (close proximity in asset pair, amounts, and valid timeline), BUT:
         * Missing transport provenance link (LOWER(s1_tx_hash) <> LOWER(bridge_send_tx)), OR
         * Leg 2 flow residual un-reconciled (bridge_leg2_mismatch > LEG2_TIGHT_FLOW_TOLERANCE), OR
         * Non-positive observed net return (gross_arbitrage_gain <= 0, unprofitable after protocol fees).
    """
    log.info("Applying Tier A labeling rules on [candidates]...")

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW labeled_tier_a AS
        WITH evaluated AS (
            SELECT
                -- Deterministic Candidate Hash Identifier (strictly lowercase normalized)
                md5(
                    CAST(src_chain AS VARCHAR) || '_' || LOWER(s1_tx_hash) || '_' || CAST(s1_log_index AS VARCHAR) || '_' ||
                    LOWER(COALESCE(message_id, 'no_msg')) || '_' ||
                    CAST(dst_chain AS VARCHAR) || '_' || LOWER(s2_tx_hash) || '_' || CAST(s2_log_index AS VARCHAR)
                ) AS candidate_id,

                src_chain,
                dst_chain,
                bridge_family,
                canonical_pair,
                bridge_asset,
                origin_asset,

                -- Leg 1 details
                s1_block,
                s1_timestamp,
                s1_tx_hash,
                s1_log_index,
                s1_pool,
                s1_amount_in,
                s1_amount_out,

                -- Bridge details
                message_id,
                bridge_amount,
                bridge_send_time,
                bridge_receive_time,
                bridge_send_tx,
                bridge_receive_tx,

                -- Leg 2 details
                s2_block,
                s2_timestamp,
                s2_tx_hash,
                s2_log_index,
                s2_pool,
                s2_amount_in,
                s2_amount_out,

                -- Performance Metrics
                leg1_bridge_mismatch,
                bridge_leg2_mismatch,
                total_cycle_latency_sec,
                gross_arbitrage_gain,
                (gross_arbitrage_gain / NULLIF(s1_amount_in, 0)) * 100.0 AS return_on_investment_pct,

                -- Validation Booleans according to Guide Section 4.1 & 11
                (message_id IS NOT NULL AND TRIM(message_id) <> '' AND LOWER(message_id) <> 'no_msg') AS has_exact_message,
                (
                    bridge_send_time IS NOT NULL 
                    AND bridge_send_tx IS NOT NULL 
                    AND TRIM(bridge_send_tx) <> ''
                ) AS has_valid_bridge_send,
                (
                    bridge_receive_time IS NOT NULL 
                    AND bridge_receive_tx IS NOT NULL 
                    AND TRIM(bridge_receive_tx) <> ''
                ) AS has_bridge_receive_record,
                (LOWER(s1_tx_hash) = LOWER(bridge_send_tx)) AS has_atomic_leg1_provenance,
                (bridge_receive_tx IS NOT NULL AND LOWER(s2_tx_hash) = LOWER(bridge_receive_tx)) AS has_atomic_leg2_provenance,
                (
                    (bridge_receive_tx IS NOT NULL AND LOWER(s2_tx_hash) = LOWER(bridge_receive_tx))
                    OR bridge_leg2_mismatch <= {LEG2_TIGHT_FLOW_TOLERANCE}
                ) AS has_tight_leg2_flow,
                (s1_amount_in > 0 AND s1_amount_out > 0 AND s2_amount_in > 0 AND s2_amount_out > 0 AND bridge_amount > 0) AS has_valid_amounts,
                (
                    s1_timestamp <= bridge_send_time 
                    AND bridge_send_time <= s2_timestamp
                    AND total_cycle_latency_sec >= 0
                    AND (
                        bridge_receive_time IS NULL 
                        OR (bridge_send_time <= bridge_receive_time AND bridge_receive_time <= s2_timestamp)
                    )
                ) AS has_valid_timeline,
                (gross_arbitrage_gain > 0) AS has_positive_net_value
            FROM candidates
        )
        SELECT
            candidate_id,
            src_chain,
            dst_chain,
            bridge_family,
            canonical_pair,
            bridge_asset,
            origin_asset,
            
            -- Tier A Ground Truth Classification
            CASE
                -- 1. Unclear: Missing source bridge evidence, clock skew / inverted timestamps, or invalid amounts
                WHEN NOT has_valid_amounts THEN 'unclear'
                WHEN NOT has_valid_bridge_send OR NOT has_exact_message THEN 'unclear'
                WHEN NOT has_valid_timeline THEN 'unclear'

                -- 2. Positive: Valid Timeline + Exact Message + Atomic Leg1 + Reconciled Leg2 (<= 10 bps or Atomic) + Positive Net Value
                WHEN has_atomic_leg1_provenance AND has_tight_leg2_flow AND has_positive_net_value THEN 'positive'

                -- 3. Natural Hard Negative: In candidate universe with valid timeline, but unlinked, un-reconciled Leg2 flow (> 10 bps), or unprofitable
                ELSE 'natural_hard_negative'
            END AS tier_a_label,

            -- Detailed Rationale for Audit and Transparency
            CASE
                WHEN NOT has_valid_amounts THEN 'invalid_or_zero_amounts'
                WHEN NOT has_valid_bridge_send OR NOT has_exact_message THEN 'missing_bridge_send_or_message_id'
                WHEN NOT has_valid_timeline THEN 'invalid_timestamp_sequence_or_clock_skew'
                WHEN has_atomic_leg1_provenance AND has_atomic_leg2_provenance AND has_positive_net_value THEN 'exact_message_atomic_both_legs_positive_gain'
                WHEN has_atomic_leg1_provenance AND has_tight_leg2_flow AND has_positive_net_value THEN 'exact_message_atomic_leg1_tight_flow_positive_gain'
                WHEN has_atomic_leg1_provenance AND NOT has_tight_leg2_flow AND has_positive_net_value THEN 'hard_neg_leg2_flow_residual_not_reconciled'
                WHEN has_atomic_leg1_provenance AND NOT has_positive_net_value THEN 'exact_message_atomic_link_negative_gain'
                WHEN NOT has_atomic_leg1_provenance AND has_positive_net_value THEN 'unlinked_proximity_positive_gain'
                ELSE 'unlinked_proximity_negative_gain'
            END AS label_rationale,

            -- Provenance Type descriptor
            CASE
                WHEN NOT has_valid_amounts OR NOT has_valid_bridge_send OR NOT has_exact_message OR NOT has_valid_timeline THEN 'invalid_or_skewed_record'
                WHEN has_atomic_leg1_provenance AND has_atomic_leg2_provenance THEN 'atomic_both_legs_' || COALESCE(bridge_family, 'unknown')
                WHEN has_atomic_leg1_provenance AND has_tight_leg2_flow AND has_bridge_receive_record THEN 'atomic_leg1_full_bridge_tight_leg2_' || COALESCE(bridge_family, 'unknown')
                WHEN has_atomic_leg1_provenance AND has_tight_leg2_flow THEN 'atomic_leg1_burn_mint_tight_leg2_' || COALESCE(bridge_family, 'unknown')
                WHEN has_atomic_leg1_provenance THEN 'atomic_leg1_unreconciled_leg2_' || COALESCE(bridge_family, 'unknown')
                ELSE 'proximity_candidate_no_transport_link'
            END AS provenance_type,

            s1_block,
            s1_timestamp,
            s1_tx_hash,
            s1_log_index,
            s1_pool,
            s1_amount_in,
            s1_amount_out,

            message_id,
            bridge_amount,
            bridge_send_time,
            bridge_receive_time,
            bridge_send_tx,
            bridge_receive_tx,

            s2_block,
            s2_timestamp,
            s2_tx_hash,
            s2_log_index,
            s2_pool,
            s2_amount_in,
            s2_amount_out,

            leg1_bridge_mismatch,
            bridge_leg2_mismatch,
            total_cycle_latency_sec,
            gross_arbitrage_gain,
            return_on_investment_pct
        FROM evaluated;
        """
    )


def save_labels_and_create_view(conn: duckdb.DuckDBPyConnection) -> None:
    """Save labeled dataset to Parquet deterministically and register canonical DuckDB View."""
    LABELS_DIR.mkdir(parents=True, exist_ok=True)
    tmp_parquet = LABEL_FILE.with_suffix(".parquet.tmp")

    log.info(f"Exporting labeled dataset to {LABEL_FILE} via ZSTD stream (ordered by src_chain, dst_chain, s1_timestamp, candidate_id)...")
    # Natural hierarchical sort preserves temporal & chain data locality for ZSTD compression and partition pruning,
    # with candidate_id tie-breaker guaranteeing strictly deterministic 100% reproducible SHA-256.
    conn.execute(
        f"""
        COPY (
            SELECT * FROM labeled_tier_a
            ORDER BY src_chain, dst_chain, s1_timestamp, candidate_id
        )
        TO '{tmp_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    if LABEL_FILE.exists():
        LABEL_FILE.unlink()
    tmp_parquet.replace(LABEL_FILE)

    log.info("Creating canonical [labels_tier_a] View in DuckDB...")
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW labels_tier_a AS
        SELECT *
        FROM read_parquet('{LABEL_FILE.as_posix()}');
        """
    )


def compute_label_statistics(conn: duckdb.DuckDBPyConnection, metrics: TierALabelingMetrics) -> None:
    """Compute aggregate distribution metrics directly from the materialized labels_tier_a View for high efficiency."""
    # Overall counts - query directly from labels_tier_a (reading precomputed Parquet)
    counts = conn.execute(
        """
        SELECT
            tier_a_label,
            COUNT(*) AS cnt
        FROM labels_tier_a
        GROUP BY tier_a_label;
        """
    ).fetchall()

    for lbl, count in counts:
        if lbl == "positive":
            metrics.positives = count
        elif lbl == "natural_hard_negative":
            metrics.natural_hard_negatives = count
        elif lbl == "unclear":
            metrics.unclear = count

    metrics.total_candidates = metrics.positives + metrics.natural_hard_negatives + metrics.unclear

    # By Label Rationale
    rat_rows = conn.execute(
        """
        SELECT
            label_rationale,
            COUNT(*)
        FROM labels_tier_a
        GROUP BY label_rationale;
        """
    ).fetchall()
    for rat, cnt in rat_rows:
        metrics.by_rationale[rat] = cnt

    # By Provenance Type
    prov_rows = conn.execute(
        """
        SELECT
            provenance_type,
            COUNT(*)
        FROM labels_tier_a
        GROUP BY provenance_type;
        """
    ).fetchall()
    for prov, cnt in prov_rows:
        metrics.by_provenance_type[prov] = cnt

    # By Bridge Family
    fam_rows = conn.execute(
        """
        SELECT
            bridge_family,
            tier_a_label,
            COUNT(*)
        FROM labels_tier_a
        GROUP BY bridge_family, tier_a_label;
        """
    ).fetchall()
    for fam, lbl, cnt in fam_rows:
        metrics.by_bridge_family.setdefault(fam, {})[lbl] = cnt

    # By Chain Pair
    pair_rows = conn.execute(
        """
        SELECT
            CAST(src_chain AS VARCHAR) || '_to_' || CAST(dst_chain AS VARCHAR),
            tier_a_label,
            COUNT(*)
        FROM labels_tier_a
        GROUP BY src_chain, dst_chain, tier_a_label;
        """
    ).fetchall()
    for pair, lbl, cnt in pair_rows:
        metrics.by_chain_pair.setdefault(pair, {})[lbl] = cnt


def main() -> None:
    t0 = time.time()
    metrics = TierALabelingMetrics()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LABELS_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== STEP 5: TIER A LABELING (PROVENANCE ANCHORS & NATURAL HARD NEGATIVES) ===")

    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)

    try:
        if not ensure_candidates_view(conn):
            log.error("Aborting: candidates dataset not ready. Run Step 4 first.")
            return

        generate_tier_a_labels(conn)
        save_labels_and_create_view(conn)
        compute_label_statistics(conn, metrics)

        metrics.wall_time_seconds = round(time.time() - t0, 3)
        metrics.peak_rss_bytes = get_peak_rss()
        metrics.label_file_sha256 = compute_file_sha256(LABEL_FILE)

        # Build Summary Report
        valid_gt_total = metrics.positives + metrics.natural_hard_negatives
        summary = {
            "experiment": "cohort_8w",
            "task": "tier_a_labeling",
            "status": "completed",
            "database_file": str(DB_FILE),
            "label_file": str(LABEL_FILE),
            "label_file_sha256": metrics.label_file_sha256,
            "wall_time_seconds": metrics.wall_time_seconds,
            "peak_rss_bytes": metrics.peak_rss_bytes,
            "thresholds": {
                "leg2_tight_flow_tolerance": LEG2_TIGHT_FLOW_TOLERANCE,
            },
            "counts": {
                "total_candidates": metrics.total_candidates,
                "positive": metrics.positives,
                "natural_hard_negative": metrics.natural_hard_negatives,
                "unclear": metrics.unclear,
                "positive_rate_all": (
                    metrics.positives / metrics.total_candidates if metrics.total_candidates > 0 else 0.0
                ),
                "positive_rate_valid_ground_truth": (
                    metrics.positives / valid_gt_total if valid_gt_total > 0 else 0.0
                ),
            },
            "by_rationale": metrics.by_rationale,
            "by_provenance_type": metrics.by_provenance_type,
            "by_bridge_family": metrics.by_bridge_family,
            "by_chain_pair": metrics.by_chain_pair,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(SUMMARY_FILE, summary)

        log.info(f"=== TIER A LABELING COMPLETED IN {metrics.wall_time_seconds:.2f}s ===")
        log.info(f"Total Evaluated: {metrics.total_candidates:,}")
        log.info(f"Positives: {metrics.positives:,} ({(metrics.positives / max(metrics.total_candidates, 1) * 100):.4f}%)")
        log.info(f"Natural Hard Negatives: {metrics.natural_hard_negatives:,}")
        log.info(f"Unclear / Excluded: {metrics.unclear:,}")
        log.info(f"Label File SHA-256: {metrics.label_file_sha256}")
        log.info(f"Labels saved to: {LABEL_FILE}")
        log.info(f"Summary written to: {SUMMARY_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
