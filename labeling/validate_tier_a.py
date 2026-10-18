from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
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
log = logging.getLogger("tier_a_validator")

# Directories and Paths
ARTIFACT_DIR       = Path("artifact")
PARQUET_DIR        = ARTIFACT_DIR / "parquet"
DUCKDB_DIR         = ARTIFACT_DIR / "duckdb"
LABELS_DIR         = ARTIFACT_DIR / "labels"
RESULT_DIR         = ARTIFACT_DIR / "results"
MANIFEST_DIR       = ARTIFACT_DIR / "manifests"

DB_FILE            = DUCKDB_DIR / "xmev.duckdb"
CONFIG_FILE        = MANIFEST_DIR / "mvp_config.json"
LABEL_FILE         = LABELS_DIR / "tier_a_labels.parquet"
STEP5_SUMMARY_FILE = RESULT_DIR / "step5_tier_a_labeling_summary.json"
VALIDATION_REPORT  = RESULT_DIR / "step5_tier_a_validation_report.json"
FROZEN_MANIFEST    = MANIFEST_DIR / "tier_a_frozen_manifest.json"
TMP_DIR            = DUCKDB_DIR / "tmp"

LEG2_TIGHT_FLOW_TOLERANCE = 0.001

ALLOWED_LABELS = ("positive", "natural_hard_negative", "unclear")
ALLOWED_RATIONALES = (
    "invalid_or_zero_amounts",
    "missing_bridge_send_or_message_id",
    "invalid_timestamp_sequence_or_clock_skew",
    "missing_or_failed_provenance_receipt",
    "exact_message_atomic_both_legs_positive_net_gain",
    "exact_message_atomic_leg1_tight_flow_positive_net_gain",
    "hard_neg_leg2_flow_residual_not_reconciled",
    "exact_message_atomic_link_positive_gross_negative_net_after_gas",
    "exact_message_atomic_link_negative_gross_gain",
    "unlinked_proximity_positive_gain",
    "unlinked_proximity_negative_gain",
)


@dataclass
class CheckResult:
    gate_id: str
    name: str
    passed: bool
    violations_or_diff: int
    details: str


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
    """Compute SHA-256 hash of a file on disk."""
    if not path.exists():
        return ""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_git_commit() -> str:
    """Retrieve current Git commit hash if inside a repository."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True, timeout=3
        ).strip()
        return out if out else "unknown"
    except Exception:
        return "unknown"


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    conn.execute("SET threads = 1;")
    conn.execute("SET memory_limit = '3GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def ensure_candidates_view(conn: duckdb.DuckDBPyConnection) -> bool:
    """Ensure that the candidates table/view exists and has rows."""
    try:
        cnt = conn.execute("SELECT COUNT(*) FROM candidates;").fetchone()[0]
        return cnt > 0
    except Exception:
        candidate_parquets = list(PARQUET_DIR.glob("candidates/*/*/*/*.parquet"))
        if not candidate_parquets:
            log.error("No candidate parquet files found in artifact/parquet/candidates/.")
            return False
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
        return cnt > 0


def run_validation_suite(conn: duckdb.DuckDBPyConnection, summary_data: dict[str, Any]) -> list[CheckResult]:
    results: list[CheckResult] = []

    # Ensure upstream candidates view is available for comparison
    if not ensure_candidates_view(conn):
        raise RuntimeError("Cannot initialize [candidates] view from parquet files.")

    # Register permanent view reading directly from Parquet file (independent, non-breaking dependency)
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW labels_tier_a AS
        SELECT * FROM read_parquet('{LABEL_FILE.as_posix()}');
        """
    )

    # Register temporary session view with physical file_row_number to inspect exact disk layout
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW labels_tier_a_raw AS
        SELECT * FROM read_parquet('{LABEL_FILE.as_posix()}', file_row_number = true);
        """
    )

    # -------------------------------------------------------------------------
    # G1: Row Conservation, Sub-Dictionary Sum Consistency & Set Equality with [candidates]
    # -------------------------------------------------------------------------
    cand_cnt = conn.execute("SELECT COUNT(*) FROM candidates;").fetchone()[0]
    lbl_cnt = conn.execute("SELECT COUNT(*) FROM labels_tier_a;").fetchone()[0]
    counts_dict = summary_data.get("counts", {})
    sum_cnt = int(counts_dict.get("total_candidates", -1))
    sum_parts = (
        int(counts_dict.get("positive", 0))
        + int(counts_dict.get("natural_hard_negative", 0))
        + int(counts_dict.get("unclear", 0))
    )

    sum_rationale = sum(int(v) for v in summary_data.get("by_rationale", {}).values())
    sum_prov = sum(int(v) for v in summary_data.get("by_provenance_type", {}).values())
    sum_fam = sum(
        int(cnt)
        for fam_map in summary_data.get("by_bridge_family", {}).values()
        for cnt in fam_map.values()
    )
    sum_pair = sum(
        int(cnt)
        for pair_map in summary_data.get("by_chain_pair", {}).values()
        for cnt in pair_map.values()
    )

    # Bidirectional EXCEPT ALL check across original candidate columns plus enriched columns
    set_diff_cnt = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM (
                SELECT src_chain, dst_chain, bridge_family, canonical_pair, bridge_asset, origin_asset,
                       s1_block, s1_timestamp, s1_tx_hash, s1_log_index, s1_pool, s1_amount_in, s1_amount_out,
                       message_id, bridge_amount, bridge_send_time, bridge_receive_time, bridge_send_tx, bridge_receive_tx,
                       s2_block, s2_timestamp, s2_tx_hash, s2_log_index, s2_pool, s2_amount_in, s2_amount_out,
                       leg1_bridge_mismatch, bridge_leg2_mismatch, total_cycle_latency_sec, gross_arbitrage_gain,
                       s1_actor_eoa, s2_actor_eoa, total_gas_origin_asset, net_arbitrage_gain, receipt_data_quality
                FROM candidates
                EXCEPT ALL
                SELECT src_chain, dst_chain, bridge_family, canonical_pair, bridge_asset, origin_asset,
                       s1_block, s1_timestamp, s1_tx_hash, s1_log_index, s1_pool, s1_amount_in, s1_amount_out,
                       message_id, bridge_amount, bridge_send_time, bridge_receive_time, bridge_send_tx, bridge_receive_tx,
                       s2_block, s2_timestamp, s2_tx_hash, s2_log_index, s2_pool, s2_amount_in, s2_amount_out,
                       leg1_bridge_mismatch, bridge_leg2_mismatch, total_cycle_latency_sec, gross_arbitrage_gain,
                       s1_actor_eoa, s2_actor_eoa, total_gas_origin_asset, net_arbitrage_gain, receipt_data_quality
                FROM labels_tier_a
            ))
            +
            (SELECT COUNT(*) FROM (
                SELECT src_chain, dst_chain, bridge_family, canonical_pair, bridge_asset, origin_asset,
                       s1_block, s1_timestamp, s1_tx_hash, s1_log_index, s1_pool, s1_amount_in, s1_amount_out,
                       message_id, bridge_amount, bridge_send_time, bridge_receive_time, bridge_send_tx, bridge_receive_tx,
                       s2_block, s2_timestamp, s2_tx_hash, s2_log_index, s2_pool, s2_amount_in, s2_amount_out,
                       leg1_bridge_mismatch, bridge_leg2_mismatch, total_cycle_latency_sec, gross_arbitrage_gain,
                       s1_actor_eoa, s2_actor_eoa, total_gas_origin_asset, net_arbitrage_gain, receipt_data_quality
                FROM labels_tier_a
                EXCEPT ALL
                SELECT src_chain, dst_chain, bridge_family, canonical_pair, bridge_asset, origin_asset,
                       s1_block, s1_timestamp, s1_tx_hash, s1_log_index, s1_pool, s1_amount_in, s1_amount_out,
                       message_id, bridge_amount, bridge_send_time, bridge_receive_time, bridge_send_tx, bridge_receive_tx,
                       s2_block, s2_timestamp, s2_tx_hash, s2_log_index, s2_pool, s2_amount_in, s2_amount_out,
                       leg1_bridge_mismatch, bridge_leg2_mismatch, total_cycle_latency_sec, gross_arbitrage_gain,
                       s1_actor_eoa, s2_actor_eoa, total_gas_origin_asset, net_arbitrage_gain, receipt_data_quality
                FROM candidates
            ));
        """
    ).fetchone()[0]

    diff_g1 = (
        abs(cand_cnt - lbl_cnt)
        + abs(lbl_cnt - sum_cnt)
        + abs(lbl_cnt - sum_parts)
        + abs(lbl_cnt - sum_rationale)
        + abs(lbl_cnt - sum_prov)
        + abs(lbl_cnt - sum_fam)
        + abs(lbl_cnt - sum_pair)
        + int(set_diff_cnt)
    )
    results.append(
        CheckResult(
            gate_id="G1_ROW_AND_SET_CONSERVATION",
            name="Exact row conservation across candidates, parquet, all 4 summary dicts, and 35-column EXCEPT ALL",
            passed=(diff_g1 == 0 and lbl_cnt > 0),
            violations_or_diff=diff_g1,
            details=(
                f"cand={cand_cnt:,}, pq={lbl_cnt:,}, sum_tot={sum_cnt:,}, "
                f"rat={sum_rationale:,}, prov={sum_prov:,}, fam={sum_fam:,}, pair={sum_pair:,}, set_diff={set_diff_cnt}"
            ),
        )
    )

    # -------------------------------------------------------------------------
    # G2: Primary Key (candidate_id) Uniqueness & Independent Hash Recomputation
    # -------------------------------------------------------------------------
    g2_violations = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM labels_tier_a WHERE candidate_id IS NULL OR LENGTH(TRIM(candidate_id)) <> 32)
            +
            (SELECT COUNT(*) - COUNT(DISTINCT candidate_id) FROM labels_tier_a)
            +
            (SELECT COUNT(*) FROM labels_tier_a WHERE (
                candidate_id = md5(
                    CAST(src_chain AS VARCHAR) || '_' || LOWER(s1_tx_hash) || '_' || CAST(s1_log_index AS VARCHAR) || '_' ||
                    LOWER(COALESCE(message_id, 'no_msg')) || '_' ||
                    CAST(dst_chain AS VARCHAR) || '_' || LOWER(s2_tx_hash) || '_' || CAST(s2_log_index AS VARCHAR)
                )
            ) IS NOT TRUE);
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G2_CANDIDATE_ID_UNIQUENESS_AND_HASH",
            name="candidate_id is 100% unique, non-null, and matches independent MD5 recomputation",
            passed=(g2_violations == 0),
            violations_or_diff=int(g2_violations),
            details=f"Null, duplicate, or hash-mismatched candidate_id count: {g2_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G3: Vocabulary Domain, Finite Numeric Check & Label-Rationale Semantic Consistency
    # -------------------------------------------------------------------------
    g3_violations = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM labels_tier_a
        WHERE (
            tier_a_label IN {ALLOWED_LABELS}
            AND label_rationale IN {ALLOWED_RATIONALES}
            AND (
                provenance_type IN ('invalid_or_skewed_record', 'proximity_candidate_no_transport_link')
                OR provenance_type LIKE 'atomic_both_legs_%'
                OR provenance_type LIKE 'atomic_leg1_full_bridge_tight_leg2_%'
                OR provenance_type LIKE 'atomic_leg1_burn_mint_tight_leg2_%'
                OR provenance_type LIKE 'atomic_leg1_unreconciled_leg2_%'
            )
            -- Label <-> Rationale Semantic Consistency
            AND (
                (tier_a_label = 'positive' AND label_rationale IN (
                    'exact_message_atomic_both_legs_positive_net_gain',
                    'exact_message_atomic_leg1_tight_flow_positive_net_gain'
                ))
                OR
                (tier_a_label = 'natural_hard_negative' AND label_rationale IN (
                    'hard_neg_leg2_flow_residual_not_reconciled',
                    'exact_message_atomic_link_positive_gross_negative_net_after_gas',
                    'exact_message_atomic_link_negative_gross_gain',
                    'unlinked_proximity_positive_gain',
                    'unlinked_proximity_negative_gain'
                ))
                OR
                (tier_a_label = 'unclear' AND (
                    (label_rationale IN (
                        'invalid_or_zero_amounts',
                        'missing_bridge_send_or_message_id',
                        'invalid_timestamp_sequence_or_clock_skew'
                    ) AND provenance_type = 'invalid_or_skewed_record')
                    OR
                    (label_rationale = 'missing_or_failed_provenance_receipt')
                ))
            )
            -- Actor existence
            AND s1_actor_eoa IS NOT NULL
            AND s2_actor_eoa IS NOT NULL
            -- Finite Numerics
            AND isfinite(s1_amount_in) AND isfinite(s1_amount_out)
            AND isfinite(s2_amount_in) AND isfinite(s2_amount_out)
            AND isfinite(bridge_amount)
            AND isfinite(leg1_bridge_mismatch) AND isfinite(bridge_leg2_mismatch)
            AND isfinite(total_cycle_latency_sec)
            AND isfinite(gross_arbitrage_gain)
            AND isfinite(net_arbitrage_gain)
            AND isfinite(total_gas_usd)
            AND isfinite(total_gas_origin_asset)
            AND (s1_amount_in <= 0 OR isfinite(return_on_investment_pct))
            AND (s1_amount_in <= 0 OR isfinite(net_roi_pct))
        ) IS NOT TRUE;
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G3_DOMAIN_AND_FINITE_NUMERICS",
            name="Allowed vocabulary, finite numerics, actor existence, and 100% semantic consistency between label and rationale",
            passed=(g3_violations == 0),
            violations_or_diff=int(g3_violations),
            details=f"Vocabulary, semantic consistency, actor, or NaN/Infinity violations: {g3_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G4: Section 4.1 Positive Strict Invariants + Economic & Gas Consistency
    # -------------------------------------------------------------------------
    g4_violations = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM labels_tier_a
        WHERE tier_a_label = 'positive'
          AND (
              src_chain <> dst_chain
              AND LOWER(s1_tx_hash) <> LOWER(s2_tx_hash)
              AND message_id IS NOT NULL AND TRIM(message_id) <> '' AND LOWER(message_id) <> 'no_msg'
              AND bridge_send_time IS NOT NULL AND bridge_send_tx IS NOT NULL AND TRIM(bridge_send_tx) <> ''
              AND LOWER(s1_tx_hash) = LOWER(bridge_send_tx)
              AND (
                  (bridge_receive_tx IS NOT NULL AND LOWER(s2_tx_hash) = LOWER(bridge_receive_tx))
                  OR (bridge_leg2_mismatch >= 0.0 AND bridge_leg2_mismatch <= {LEG2_TIGHT_FLOW_TOLERANCE})
              )
              AND s1_amount_in > 0 AND s1_amount_out > 0
              AND s2_amount_in > 0 AND s2_amount_out > 0
              AND bridge_amount > 0
              -- Verified receipts & gas on both legs
              AND receipt_data_quality = 'both_receipts_verified'
              AND s1_gas_native > 0 AND s2_gas_native > 0 AND total_gas_origin_asset > 0
              AND gross_arbitrage_gain > 0 AND net_arbitrage_gain > 0
              AND ABS(net_arbitrage_gain - (gross_arbitrage_gain - total_gas_origin_asset)) <= 1e-6
              AND s1_actor_eoa IS NOT NULL AND s1_first_called_contract IS NOT NULL
              AND s2_actor_eoa IS NOT NULL AND s2_first_called_contract IS NOT NULL
              AND (s1_amount_in <= 0 OR return_on_investment_pct > 0)
              AND (s1_amount_in <= 0 OR net_roi_pct > 0)
              AND ABS(return_on_investment_pct - ((gross_arbitrage_gain / s1_amount_in) * 100.0)) <= 1e-4
              AND ABS(net_roi_pct - ((net_arbitrage_gain / s1_amount_in) * 100.0)) <= 1e-4
          ) IS NOT TRUE;
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G4_POSITIVE_PROVENANCE_AND_ECONOMIC_INVARIANT",
            name="Every 'positive' satisfies cross-chain, atomic leg1, verified receipts, positive net gain after gas, and actors",
            passed=(g4_violations == 0),
            violations_or_diff=int(g4_violations),
            details=f"Positive records failing strict provenance or economic/gas invariants: {g4_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G5: Valid Ground Truth (Positive & Hard Negative) Prerequisite Invariants
    # -------------------------------------------------------------------------
    g5_violations = conn.execute(
        """
        SELECT COUNT(*)
        FROM labels_tier_a
        WHERE tier_a_label IN ('positive', 'natural_hard_negative')
          AND (
              src_chain <> dst_chain
              AND s1_amount_in > 0 AND s1_amount_out > 0
              AND s2_amount_in > 0 AND s2_amount_out > 0
              AND bridge_amount > 0
              AND message_id IS NOT NULL AND TRIM(message_id) <> '' AND LOWER(message_id) <> 'no_msg'
              AND bridge_send_time IS NOT NULL AND bridge_send_tx IS NOT NULL AND TRIM(bridge_send_tx) <> ''
              AND s1_timestamp <= bridge_send_time
              AND bridge_send_time <= s2_timestamp
              AND total_cycle_latency_sec >= 0
              AND ABS(total_cycle_latency_sec - (s2_timestamp - s1_timestamp)) <= 1e-6
              AND (
                  bridge_receive_time IS NULL
                  OR (bridge_send_time <= bridge_receive_time AND bridge_receive_time <= s2_timestamp)
              )
          ) IS NOT TRUE;
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G5_GROUND_TRUTH_VALIDITY_INVARIANT",
            name="All positive and natural_hard_negative records have valid amounts, message_id, and causal timeline",
            passed=(g5_violations == 0),
            violations_or_diff=int(g5_violations),
            details=f"Ground truth records violating amount, message, or timeline prerequisites: {g5_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G6: Bidirectional Unclear Equivalence (iff condition)
    # -------------------------------------------------------------------------
    g6_violations = conn.execute(
        """
        SELECT COUNT(*)
        FROM labels_tier_a
        WHERE (tier_a_label = 'unclear') IS DISTINCT FROM (
            (
                s1_amount_in > 0 AND s1_amount_out > 0
                AND s2_amount_in > 0 AND s2_amount_out > 0
                AND bridge_amount > 0
                AND message_id IS NOT NULL AND TRIM(message_id) <> '' AND LOWER(message_id) <> 'no_msg'
                AND bridge_send_time IS NOT NULL AND bridge_send_tx IS NOT NULL AND TRIM(bridge_send_tx) <> ''
                AND s1_timestamp <= bridge_send_time
                AND bridge_send_time <= s2_timestamp
                AND total_cycle_latency_sec >= 0
                AND (
                    bridge_receive_time IS NULL
                    OR (bridge_send_time <= bridge_receive_time AND bridge_receive_time <= s2_timestamp)
                )
                AND (
                    (LOWER(s1_tx_hash) = LOWER(bridge_send_tx)) IS NOT TRUE
                    OR (
                        receipt_data_quality = 'both_receipts_verified'
                        AND s1_gas_native IS NOT NULL AND s1_gas_native >= 0
                        AND s2_gas_native IS NOT NULL AND s2_gas_native >= 0
                        AND total_gas_origin_asset IS NOT NULL AND total_gas_origin_asset >= 0
                    )
                )
            ) IS NOT TRUE
        );
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G6_UNCLEAR_BIDIRECTIONAL_EQUIVALENCE",
            name="A record is labeled 'unclear' if and only if it violates amounts, bridge send, timeline, or provenance receipts",
            passed=(g6_violations == 0),
            violations_or_diff=int(g6_violations),
            details=f"Bidirectional equivalence mismatches for 'unclear': {g6_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G7: Bidirectional Partition Between Positive and Natural Hard Negative
    # -------------------------------------------------------------------------
    g7_violations = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM labels_tier_a
        WHERE tier_a_label IN ('positive', 'natural_hard_negative')
          AND (tier_a_label = 'positive') IS DISTINCT FROM (
              (
                  LOWER(s1_tx_hash) = LOWER(bridge_send_tx)
                  AND (
                      receipt_data_quality = 'both_receipts_verified'
                      AND s1_gas_native IS NOT NULL AND s1_gas_native >= 0
                      AND s2_gas_native IS NOT NULL AND s2_gas_native >= 0
                      AND total_gas_origin_asset IS NOT NULL AND total_gas_origin_asset >= 0
                  )
                  AND (
                      (bridge_receive_tx IS NOT NULL AND LOWER(s2_tx_hash) = LOWER(bridge_receive_tx))
                      OR (bridge_leg2_mismatch >= 0.0 AND bridge_leg2_mismatch <= {LEG2_TIGHT_FLOW_TOLERANCE})
                  )
                  AND net_arbitrage_gain > 0
              ) IS TRUE
          );
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G7_POSITIVE_HARD_NEGATIVE_PARTITION",
            name="Exact mutual exclusion between 'positive' and 'natural_hard_negative' on valid ground truth",
            passed=(g7_violations == 0),
            violations_or_diff=int(g7_violations),
            details=f"Boundary classification errors between positive and hard negative: {g7_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G8: Deterministic Physical Ordering in Parquet using file_row_number
    # -------------------------------------------------------------------------
    g8_violations = conn.execute(
        """
        WITH ordered_check AS (
            SELECT
                file_row_number,
                src_chain,
                dst_chain,
                s1_timestamp,
                candidate_id,
                LAG(src_chain) OVER (ORDER BY file_row_number) AS prev_src,
                LAG(dst_chain) OVER (ORDER BY file_row_number) AS prev_dst,
                LAG(s1_timestamp) OVER (ORDER BY file_row_number) AS prev_ts,
                LAG(candidate_id) OVER (ORDER BY file_row_number) AS prev_id
            FROM labels_tier_a_raw
        )
        SELECT COUNT(*)
        FROM ordered_check
        WHERE prev_src IS NOT NULL
          AND (
              src_chain < prev_src
              OR (src_chain = prev_src AND dst_chain < prev_dst)
              OR (src_chain = prev_src AND dst_chain = prev_dst AND s1_timestamp < prev_ts)
              OR (src_chain = prev_src AND dst_chain = prev_dst AND s1_timestamp = prev_ts AND candidate_id <= prev_id)
          );
        """
    ).fetchone()[0]
    results.append(
        CheckResult(
            gate_id="G8_PHYSICAL_PARQUET_SORT_ORDER",
            name="Physical Parquet file rows are strictly ordered by (src_chain, dst_chain, s1_timestamp, candidate_id)",
            passed=(g8_violations == 0),
            violations_or_diff=int(g8_violations),
            details=f"Out-of-order or duplicate physical row transitions: {g8_violations}",
        )
    )

    # -------------------------------------------------------------------------
    # G9: Physical File SHA-256 Checksum Match
    # -------------------------------------------------------------------------
    actual_sha256 = compute_file_sha256(LABEL_FILE)
    expected_sha256 = summary_data.get("label_file_sha256", "")
    sha_matched = (actual_sha256 == expected_sha256 and len(actual_sha256) == 64)
    results.append(
        CheckResult(
            gate_id="G9_SHA256_INTEGRITY",
            name="Physical Parquet SHA-256 matches step5 summary record",
            passed=sha_matched,
            violations_or_diff=(0 if sha_matched else 1),
            details=f"actual={actual_sha256[:16]}..., expected={expected_sha256[:16]}...",
        )
    )

    # -------------------------------------------------------------------------
    # G10: Multi-Bridge Coverage Viability (Across & CCTP have Positives & Hard Negatives)
    # -------------------------------------------------------------------------
    bridge_pos = conn.execute(
        """
        SELECT bridge_family,
               SUM(CASE WHEN tier_a_label = 'positive' THEN 1 ELSE 0 END) AS pos_cnt,
               SUM(CASE WHEN tier_a_label = 'natural_hard_negative' THEN 1 ELSE 0 END) AS neg_cnt
        FROM labels_tier_a
        GROUP BY bridge_family;
        """
    ).fetchall()
    fam_stats = {row[0]: (int(row[1]), int(row[2])) for row in bridge_pos}
    across_ok = fam_stats.get("across", (0, 0))[0] > 0 and fam_stats.get("across", (0, 0))[1] > 0
    cctp_ok = fam_stats.get("cctp", (0, 0))[0] > 0 and fam_stats.get("cctp", (0, 0))[1] > 0
    g10_passed = across_ok and cctp_ok
    results.append(
        CheckResult(
            gate_id="G10_LOBO_BRIDGE_VIABILITY",
            name="Active bridge families ('across', 'cctp') both contain positive and hard-negative anchors",
            passed=g10_passed,
            violations_or_diff=(0 if g10_passed else 1),
            details=f"Bridge coverage (pos, neg): {fam_stats}",
        )
    )

    return results


def compute_content_digest_streaming(conn: duckdb.DuckDBPyConnection) -> str:
    """
    Compute deterministic MD5 digest over all records in physical Parquet order
    using low-memory Python cursor batching (no global SQL string_agg or ORDER BY in RAM).
    """
    hasher = hashlib.md5()
    cursor = conn.execute(
        """
        SELECT candidate_id || ':' || tier_a_label || ':' || label_rationale
        FROM labels_tier_a_raw
        ORDER BY file_row_number;
        """
    )
    first = True
    while rows := cursor.fetchmany(10000):
        chunk_str = "|".join(r[0] for r in rows)
        if not first:
            hasher.update(b"|")
        hasher.update(chunk_str.encode("utf-8"))
        first = False
    return hasher.hexdigest()


def get_parquet_schema_signature(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Extract column names, types, and deterministic schema hash from Parquet."""
    desc_rows = conn.execute("DESCRIBE SELECT * FROM labels_tier_a;").fetchall()
    columns = [{"name": str(r[0]), "type": str(r[1])} for r in desc_rows]
    schema_str = "|".join(f"{c['name']}:{c['type']}" for c in columns)
    schema_sha256 = hashlib.sha256(schema_str.encode("utf-8")).hexdigest()
    return {
        "column_count": len(columns),
        "schema_sha256": schema_sha256,
        "columns": columns,
    }


def resolve_labeling_script_path() -> Path:
    """Locate the Tier A labeling script in project paths."""
    candidates = [
        Path(__file__).resolve().parent / "label_tier_a.py",
        Path("labeling/label_tier_a.py"),
        Path("craw_normalize/step5_tier_a_labeling.py"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return Path("labeling/label_tier_a.py")


def main() -> None:
    t0 = time.time()
    log.info("=== STEP 5 TASK 2: TIER A VALIDATION SUITE & CHECKSUM FREEZE ===")

    if not LABEL_FILE.exists():
        log.error(f"Missing label parquet file: {LABEL_FILE}")
        sys.exit(1)
    if not STEP5_SUMMARY_FILE.exists():
        log.error(f"Missing step5 summary file: {STEP5_SUMMARY_FILE}")
        sys.exit(1)

    summary_data = json.loads(STEP5_SUMMARY_FILE.read_text(encoding="utf-8"))
    mvp_cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8")) if CONFIG_FILE.exists() else {}

    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)

    try:
        results = run_validation_suite(conn, summary_data)
        all_passed = all(r.passed for r in results)

        for r in results:
            status_str = "PASS" if r.passed else "FAIL"
            log.info(f"[{status_str}] {r.gate_id}: {r.name} ({r.details})")

        content_digest_md5 = compute_content_digest_streaming(conn)
        schema_sig = get_parquet_schema_signature(conn)

        # Drop only the temporary session view after streaming digest completes
        conn.execute("DROP VIEW IF EXISTS labels_tier_a_raw;")

        parquet_sha256 = compute_file_sha256(LABEL_FILE)
        summary_sha256 = compute_file_sha256(STEP5_SUMMARY_FILE)
        config_sha256 = compute_file_sha256(CONFIG_FILE)
        labeling_script = resolve_labeling_script_path()
        labeling_script_sha256 = compute_file_sha256(labeling_script)
        validator_script_sha256 = compute_file_sha256(Path(__file__))
        parquet_size_bytes = LABEL_FILE.stat().st_size

        wall_time = round(time.time() - t0, 3)
        peak_rss = get_peak_rss()

        report_payload = {
            "experiment": "cohort_8w",
            "task": "tier_a_validation_and_freeze",
            "overall_status": "PASSED" if all_passed else "FAILED",
            "wall_time_seconds": wall_time,
            "peak_rss_bytes": peak_rss,
            "gates_total": len(results),
            "gates_passed": sum(1 for r in results if r.passed),
            "gates_failed": sum(1 for r in results if not r.passed),
            "gate_results": [
                {
                    "gate_id": r.gate_id,
                    "name": r.name,
                    "passed": r.passed,
                    "violations_or_diff": r.violations_or_diff,
                    "details": r.details,
                }
                for r in results
            ],
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(VALIDATION_REPORT, report_payload)
        report_sha256 = compute_file_sha256(VALIDATION_REPORT)

        if not all_passed:
            log.error("Validation FAILED! Frozen manifest will NOT be generated. Inspect report for details.")
            sys.exit(1)

        # Determine active vs excluded bridges and zero-positive chain pairs for Claim Gate (Section 1.1 & 4.3)
        by_fam = summary_data.get("by_bridge_family", {})
        active_bridges = sorted(list(by_fam.keys()))
        configured_bridges = [b.lower() for b in mvp_cfg.get("bridge_families", ["CCTP", "Across", "Stargate"])]
        excluded_bridges = sorted([b for b in configured_bridges if b not in active_bridges])

        by_pair = summary_data.get("by_chain_pair", {})
        pairs_with_positives = sorted([p for p, m in by_pair.items() if int(m.get("positive", 0)) > 0])
        pairs_zero_positives = sorted([p for p, m in by_pair.items() if int(m.get("positive", 0)) == 0])

        # Freeze manifest in artifact/manifests/tier_a_frozen_manifest.json (Guide Section 12.1)
        frozen_payload = {
            "manifest_version": "1.1.0",
            "experiment": "cohort_8w",
            "stage": "step5_tier_a_ground_truth",
            "status": "FROZEN_VERIFIED",
            "environment": {
                "git_commit": get_git_commit(),
                "python_version": sys.version.split()[0],
                "duckdb_version": duckdb.__version__,
                "platform": platform.platform(),
                "cpu_count_logical": os.cpu_count(),
                "total_ram_bytes": int(psutil.virtual_memory().total),
                "validator_peak_rss_bytes": peak_rss,
                "duckdb_settings": {
                    "threads": 1,
                    "memory_limit": "3GB",
                    "preserve_insertion_order": False,
                },
            },
            "artifacts": {
                "tier_a_labels_parquet": {
                    "path": str(LABEL_FILE),
                    "sha256": parquet_sha256,
                    "size_bytes": parquet_size_bytes,
                    "physical_order_digest_md5": content_digest_md5,
                    "schema_signature": schema_sig,
                },
                "step5_summary_json": {
                    "path": str(STEP5_SUMMARY_FILE),
                    "sha256": summary_sha256,
                },
                "step5_validation_report_json": {
                    "path": str(VALIDATION_REPORT),
                    "sha256": report_sha256,
                },
                "mvp_config_json": {
                    "path": str(CONFIG_FILE),
                    "sha256": config_sha256,
                },
                "scripts": {
                    "labeling_script": {
                        "path": str(labeling_script),
                        "sha256": labeling_script_sha256,
                    },
                    "validation_script": {
                        "path": str(Path(__file__)),
                        "sha256": validator_script_sha256,
                    },
                },
            },
            "locked_parameters": summary_data.get(
                "thresholds", {"leg2_tight_flow_tolerance": LEG2_TIGHT_FLOW_TOLERANCE}
            ),
            "claim_gate_status": {
                "active_bridge_families": active_bridges,
                "excluded_bridge_families_no_public_provenance": excluded_bridges,
                "lobo_active_folds": len(active_bridges),
                "directed_pairs_with_positives": pairs_with_positives,
                "directed_pairs_zero_positives": pairs_zero_positives,
                "note": (
                    "Per Guide Section 1.1 & 4.3: Stargate excluded prior to split lock due to zero paired provenance "
                    "in window; LOBO evaluated across active families (across, cctp)."
                ),
            },
            "verified_counts": summary_data.get("counts", {}),
            "by_rationale": summary_data.get("by_rationale", {}),
            "by_provenance_type": summary_data.get("by_provenance_type", {}),
            "by_bridge_family": by_fam,
            "by_chain_pair": by_pair,
            "frozen_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(FROZEN_MANIFEST, frozen_payload)

        log.info(f"=== ALL {len(results)} VALIDATION GATES PASSED IN {wall_time:.2f}s ===")
        log.info(f"Parquet SHA-256   : {parquet_sha256}")
        log.info(f"Stream Digest MD5 : {content_digest_md5}")
        log.info(f"Schema SHA-256    : {schema_sig['schema_sha256']}")
        log.info(f"Peak RSS Memory   : {peak_rss / (1024 * 1024):.1f} MB")
        log.info(f"Validation Report : {VALIDATION_REPORT}")
        log.info(f"Frozen Manifest   : {FROZEN_MANIFEST}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
