from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
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
log = logging.getLogger("step6a_sample_tier_b")

# Directories and Paths
ARTIFACT_DIR       = Path("artifact")
DUCKDB_DIR         = ARTIFACT_DIR / "duckdb"
LABELS_DIR         = ARTIFACT_DIR / "labels"
TIER_B_DIR         = LABELS_DIR / "tier_b"
RESULT_DIR         = ARTIFACT_DIR / "results"

DB_FILE            = DUCKDB_DIR / "xmev.duckdb"
TIER_A_PARQUET     = LABELS_DIR / "tier_a_labels.parquet"
MASTER_PARQUET     = TIER_B_DIR / "tier_b_master_sample.parquet"
ANNOTATOR_1_CSV    = TIER_B_DIR / "annotator_1_blind.csv"
ANNOTATOR_2_CSV    = TIER_B_DIR / "annotator_2_blind.csv"
MANUAL_MD          = TIER_B_DIR / "ANNOTATION_MANUAL.md"
SUMMARY_FILE       = RESULT_DIR / "step6a_tier_b_sampling_summary.json"
TMP_DIR            = DUCKDB_DIR / "tmp"

# Fixed Constants per Guide Section 4.2 & Paper Section IV.E
SEED               = 42
TARGET_SAMPLE_SIZE = 120
MIN_STRATUM_POP    = 25  # Collapse sparse 6-way cells below this size into coarse delay/liquidity cell

EXPLORER_TX_PREFIX = {
    1: "https://etherscan.io/tx/",
    42161: "https://arbiscan.io/tx/",
    8453: "https://basescan.org/tx/",
}

BLIND_CSV_COLUMNS = [
    "sample_order",
    "candidate_id",
    "src_chain",
    "dst_chain",
    "bridge_family",
    "canonical_pair",
    "origin_asset",
    "bridge_asset",
    "s1_timestamp",
    "s1_tx_hash",
    "s1_explorer_uri",
    "s1_actor_eoa",
    "s1_first_called_contract",
    "s1_beneficiary",
    "s1_amount_in",
    "s1_amount_out",
    "s1_gas_usd",
    "message_id",
    "bridge_amount",
    "bridge_send_time",
    "bridge_receive_time",
    "bridge_send_tx",
    "bridge_receive_tx",
    "bridge_send_explorer_uri",
    "bridge_receive_explorer_uri",
    "s2_timestamp",
    "s2_tx_hash",
    "s2_explorer_uri",
    "s2_actor_eoa",
    "s2_first_called_contract",
    "s2_beneficiary",
    "s2_amount_in",
    "s2_amount_out",
    "s2_gas_usd",
    "leg1_bridge_mismatch",
    "bridge_leg2_mismatch",
    "total_cycle_latency_sec",
    "gross_arbitrage_gain",
    "total_gas_origin_asset",
    "net_arbitrage_gain",
    "net_roi_pct",
    "receipt_data_quality",
    "actor_match_summary",
    "annotator_label",
    "rationale_code",
    "evidence_notes",
]


def get_peak_rss() -> int:
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
    conn.execute("SET threads = 1;")
    conn.execute("SET memory_limit = '3GB';")
    conn.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}';")
    conn.execute("SET preserve_insertion_order = false;")


def ensure_labels_tier_a_view(conn: duckdb.DuckDBPyConnection) -> None:
    if not TIER_A_PARQUET.exists():
        raise FileNotFoundError(f"Tier A labels parquet missing: {TIER_A_PARQUET}")
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW labels_tier_a AS
        SELECT * FROM read_parquet('{TIER_A_PARQUET.as_posix()}');
        """
    )


def build_tier_b_universe(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Construct the Tier B sampling universe from provenance-incomplete / inventory candidates
    (tier_a_label = 'natural_hard_negative') where on-chain receipt metadata is available
    (receipt_data_quality IN ('both_receipts_verified', 'partial_receipt_verified')),
    and compute all 6 stratification axes per Guide Section 4.2 & Paper Section IV.E:
      1. chain_pair
      2. bridge_family
      3. delay_decile (1..10 via NTILE(10) on total_cycle_latency_sec)
      4. pool_liquidity_tier (high_liquidity_pool vs standard_liquidity_pool)
      5. actor_recurrence_bin (recurring_actor vs one_off_actor)
      6. preliminary_score & score_tier (high, medium, low)
    """
    log.info("1. Building Tier B sampling universe and computing 6 stratification axes...")

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE tier_b_universe_raw AS
        WITH eligible AS (
            SELECT
                *,
                CAST(src_chain AS VARCHAR) || '_to_' || CAST(dst_chain AS VARCHAR) AS chain_pair,
                COALESCE(LOWER(s1_actor_eoa), LOWER(s1_beneficiary), 'unknown_actor') AS primary_actor_cluster,
                (
                    (s1_actor_eoa IS NOT NULL AND s2_actor_eoa IS NOT NULL AND LOWER(s1_actor_eoa) = LOWER(s2_actor_eoa))
                    OR (s1_beneficiary IS NOT NULL AND s2_beneficiary IS NOT NULL AND LOWER(s1_beneficiary) = LOWER(s2_beneficiary))
                    OR (
                        s1_first_called_contract IS NOT NULL
                        AND s2_first_called_contract IS NOT NULL
                        AND LOWER(s1_first_called_contract) = LOWER(s2_first_called_contract)
                    )
                    OR (s1_actor_eoa IS NOT NULL AND s2_beneficiary IS NOT NULL AND LOWER(s1_actor_eoa) = LOWER(s2_beneficiary))
                    OR (s1_beneficiary IS NOT NULL AND s2_actor_eoa IS NOT NULL AND LOWER(s1_beneficiary) = LOWER(s2_actor_eoa))
                ) AS actor_match_summary,
                1.0 / (1.0 + COALESCE(leg1_bridge_mismatch, 1.0) + COALESCE(bridge_leg2_mismatch, 1.0)) AS flow_tightness_score
            FROM labels_tier_a
            WHERE tier_a_label = 'natural_hard_negative'
              AND receipt_data_quality IN ('both_receipts_verified', 'partial_receipt_verified')
        ),
        enriched_axes AS (
            SELECT
                *,
                NTILE(10) OVER (ORDER BY total_cycle_latency_sec ASC, candidate_id ASC) AS delay_decile,
                COUNT(*) OVER (PARTITION BY LOWER(s1_pool), LOWER(s2_pool)) AS pool_pair_freq,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY LOWER(s1_pool), LOWER(s2_pool)) >= 500
                    THEN 'high_liquidity_pool'
                    ELSE 'standard_liquidity_pool'
                END AS pool_liquidity_tier,
                COUNT(*) OVER (PARTITION BY primary_actor_cluster) AS actor_cluster_freq,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY primary_actor_cluster) > 1
                    THEN 'recurring_actor'
                    ELSE 'one_off_actor'
                END AS actor_recurrence_bin,
                ROUND(
                    (CASE WHEN actor_match_summary THEN 0.50 ELSE 0.0 END)
                    + (0.25 * flow_tightness_score)
                    + (CASE WHEN net_arbitrage_gain > 0 THEN 0.25 ELSE 0.0 END),
                    6
                ) AS preliminary_score
            FROM eligible
        ),
        scored AS (
            SELECT
                *,
                CASE
                    WHEN preliminary_score >= 0.75 THEN 'high'
                    WHEN preliminary_score >= 0.45 THEN 'medium'
                    ELSE 'low'
                END AS score_tier,
                CASE
                    WHEN delay_decile <= 5 THEN 'd1_d5'
                    ELSE 'd6_d10'
                END AS delay_half
            FROM enriched_axes
        ),
        fine_strata AS (
            SELECT
                *,
                chain_pair || '__' || bridge_family || '__' || delay_half || '__' ||
                pool_liquidity_tier || '__' || actor_recurrence_bin || '__' || score_tier AS fine_stratum_id,
                COUNT(*) OVER (
                    PARTITION BY chain_pair, bridge_family, delay_half, pool_liquidity_tier, actor_recurrence_bin, score_tier
                ) AS fine_stratum_cnt
            FROM scored
        )
        SELECT
            *,
            CASE
                WHEN fine_stratum_cnt >= {MIN_STRATUM_POP} THEN fine_stratum_id
                ELSE chain_pair || '__' || bridge_family || '__all_delay__all_liq__' || actor_recurrence_bin || '__' || score_tier
            END AS stratum_id
        FROM fine_strata;
        """
    )

    total_univ = conn.execute("SELECT COUNT(*) FROM tier_b_universe_raw;").fetchone()[0]
    strata_cnt = conn.execute("SELECT COUNT(DISTINCT stratum_id) FROM tier_b_universe_raw;").fetchone()[0]
    log.info(f"   Tier B sampling universe: {total_univ:,} candidates across {strata_cnt} strata.")


def allocate_stratum_quotas(conn: duckdb.DuckDBPyConnection) -> dict[str, tuple[int, int]]:
    """
    Deterministic Neyman/Priority-weighted stratified allocation for N = 120:
      - Every non-empty stratum h gets at least n_h >= 1 (ensuring 100% population coverage
        and exact unbiased Horvitz-Thompson IPW sum: sum(ipw_weight) == N_universe).
      - Remaining quota (120 - H) is allocated proportionally to N_h * tier_priority_weight
        (high=3.5, medium=1.5, low=0.4) via largest-remainder method, capped at N_h.
    Returns dict: stratum_id -> (N_h, n_h).
    """
    rows = conn.execute(
        """
        SELECT
            stratum_id,
            score_tier,
            COUNT(*) AS pop_size
        FROM tier_b_universe_raw
        GROUP BY stratum_id, score_tier
        ORDER BY stratum_id ASC;
        """
    ).fetchall()

    num_strata = len(rows)
    if num_strata > TARGET_SAMPLE_SIZE:
        raise RuntimeError(
            f"Number of strata ({num_strata}) exceeds TARGET_SAMPLE_SIZE ({TARGET_SAMPLE_SIZE}). "
            f"Increase MIN_STRATUM_POP to guarantee n_h >= 1 per stratum."
        )

    tier_weights = {"high": 3.5, "medium": 1.5, "low": 0.4}

    # Initial allocation: 1 per stratum (subject to pop_size >= 1)
    alloc: dict[str, int] = {r[0]: 1 for r in rows}
    pop_map: dict[str, int] = {r[0]: int(r[2]) for r in rows}
    tier_map: dict[str, str] = {r[0]: str(r[1]) for r in rows}

    remaining = TARGET_SAMPLE_SIZE - sum(alloc.values())

    # Compute weighted capacity for remaining allocation
    weights: dict[str, float] = {}
    for sid in alloc:
        cap = pop_map[sid] - alloc[sid]
        if cap > 0:
            # Square-root damped population size * priority weight prevents a single huge stratum
            # from monopolizing the sample while favoring high/medium score tiers.
            weights[sid] = math.sqrt(pop_map[sid]) * tier_weights.get(tier_map[sid], 1.0)
        else:
            weights[sid] = 0.0

    total_w = sum(weights.values())
    if remaining > 0 and total_w > 0:
        exact_extra = {sid: (remaining * weights[sid] / total_w) for sid in alloc}
        for sid in alloc:
            add_int = min(int(math.floor(exact_extra[sid])), pop_map[sid] - alloc[sid])
            alloc[sid] += add_int

        still_rem = TARGET_SAMPLE_SIZE - sum(alloc.values())
        # Sort by fractional remainder descending, then stratum_id ascending for determinism
        remainders = sorted(
            alloc.keys(),
            key=lambda s: (
                -(exact_extra[s] - math.floor(exact_extra[s])) if alloc[s] < pop_map[s] else 1e9,
                s,
            ),
        )
        for sid in remainders:
            if still_rem <= 0:
                break
            if alloc[sid] < pop_map[sid]:
                alloc[sid] += 1
                still_rem -= 1

    total_allocated = sum(alloc.values())
    if total_allocated != TARGET_SAMPLE_SIZE:
        raise RuntimeError(f"Quota allocation mismatch: expected {TARGET_SAMPLE_SIZE}, got {total_allocated}")

    return {sid: (pop_map[sid], alloc[sid]) for sid in sorted(alloc.keys())}


def draw_stratified_sample(
    conn: duckdb.DuckDBPyConnection,
    quotas: dict[str, tuple[int, int]],
) -> None:
    """
    Draw n_h candidates from each stratum h deterministically using SEED = 42,
    prioritizing 'both_receipts_verified' within each stratum so annotators have maximum
    complete 2-leg receipt coverage, and compute exact IPW weights:
      sampling_prob = n_h / N_h
      ipw_weight    = N_h / n_h
    """
    log.info(f"2. Drawing N = {TARGET_SAMPLE_SIZE} stratified candidates with SEED = {SEED} and computing IPW weights...")

    quota_values_sql = ",\n            ".join(
        f"('{sid}', {pop_sz}, {samp_sz})"
        for sid, (pop_sz, samp_sz) in quotas.items()
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stratum_quotas AS
        SELECT * FROM (
            VALUES
            {quota_values_sql}
        ) AS t(stratum_id, stratum_pop_size, stratum_sample_size);

        CREATE OR REPLACE TEMP TABLE tier_b_sampled AS
        WITH ranked AS (
            SELECT
                u.*,
                q.stratum_pop_size,
                q.stratum_sample_size,
                CAST(q.stratum_sample_size AS DOUBLE) / CAST(q.stratum_pop_size AS DOUBLE) AS sampling_prob,
                CAST(q.stratum_pop_size AS DOUBLE) / CAST(q.stratum_sample_size AS DOUBLE) AS ipw_weight,
                ROW_NUMBER() OVER (
                    PARTITION BY u.stratum_id
                    ORDER BY
                        CASE WHEN u.receipt_data_quality = 'both_receipts_verified' THEN 0 ELSE 1 END ASC,
                        md5(u.candidate_id || '_tier_b_draw_seed_{SEED}') ASC
                ) AS rn_in_stratum
            FROM tier_b_universe_raw u
            INNER JOIN stratum_quotas q
              ON u.stratum_id = q.stratum_id
        ),
        selected AS (
            SELECT *
            FROM ranked
            WHERE rn_in_stratum <= stratum_sample_size
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY md5(candidate_id || '_blind_presentation_order_seed_{SEED}') ASC
            ) AS sample_order,
            *
        FROM selected
        ORDER BY sample_order ASC;
        """
    )

    TIER_B_DIR.mkdir(parents=True, exist_ok=True)
    tmp_pq = MASTER_PARQUET.with_suffix(".parquet.tmp")
    conn.execute(
        f"""
        COPY (
            SELECT * FROM tier_b_sampled ORDER BY sample_order ASC
        )
        TO '{tmp_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    if MASTER_PARQUET.exists():
        MASTER_PARQUET.unlink()
    tmp_pq.replace(MASTER_PARQUET)

    conn.execute(
        f"""
        CREATE OR REPLACE VIEW tier_b_master_sample AS
        SELECT * FROM read_parquet('{MASTER_PARQUET.as_posix()}');
        """
    )


def make_explorer_uri(chain_id: int, tx_hash: str | None) -> str:
    if not tx_hash or not str(tx_hash).strip():
        return ""
    prefix = EXPLORER_TX_PREFIX.get(int(chain_id), "")
    return f"{prefix}{str(tx_hash).strip().lower()}" if prefix else ""


def export_blinded_csvs(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Export two identical blinded CSV worksheets (annotator_1_blind.csv & annotator_2_blind.csv)
    with all model/heuristic scores and Tier A labels/rationales strictly removed.
    """
    log.info("3. Exporting blinded annotation CSV files for Annotator 1 and Annotator 2...")

    cursor = conn.execute(
        """
        SELECT
            sample_order,
            candidate_id,
            src_chain,
            dst_chain,
            bridge_family,
            canonical_pair,
            origin_asset,
            bridge_asset,
            s1_timestamp,
            s1_tx_hash,
            s1_actor_eoa,
            s1_first_called_contract,
            s1_beneficiary,
            s1_amount_in,
            s1_amount_out,
            s1_gas_usd,
            message_id,
            bridge_amount,
            bridge_send_time,
            bridge_receive_time,
            bridge_send_tx,
            bridge_receive_tx,
            s2_timestamp,
            s2_tx_hash,
            s2_actor_eoa,
            s2_first_called_contract,
            s2_beneficiary,
            s2_amount_in,
            s2_amount_out,
            s2_gas_usd,
            leg1_bridge_mismatch,
            bridge_leg2_mismatch,
            total_cycle_latency_sec,
            gross_arbitrage_gain,
            total_gas_origin_asset,
            net_arbitrage_gain,
            net_roi_pct,
            receipt_data_quality,
            actor_match_summary
        FROM tier_b_master_sample
        ORDER BY sample_order ASC;
        """
    )
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]

    blind_rows: list[dict[str, Any]] = []
    for r in rows:
        rec = dict(zip(col_names, r))
        src_cid = int(rec["src_chain"])
        dst_cid = int(rec["dst_chain"])

        out_row = {
            "sample_order": int(rec["sample_order"]),
            "candidate_id": str(rec["candidate_id"]),
            "src_chain": src_cid,
            "dst_chain": dst_cid,
            "bridge_family": str(rec["bridge_family"]),
            "canonical_pair": str(rec["canonical_pair"]),
            "origin_asset": str(rec["origin_asset"]),
            "bridge_asset": str(rec["bridge_asset"]),
            "s1_timestamp": int(rec["s1_timestamp"]),
            "s1_tx_hash": str(rec["s1_tx_hash"]),
            "s1_explorer_uri": make_explorer_uri(src_cid, rec["s1_tx_hash"]),
            "s1_actor_eoa": rec["s1_actor_eoa"] or "",
            "s1_first_called_contract": rec["s1_first_called_contract"] or "",
            "s1_beneficiary": rec["s1_beneficiary"] or "",
            "s1_amount_in": round(float(rec["s1_amount_in"]), 8),
            "s1_amount_out": round(float(rec["s1_amount_out"]), 8),
            "s1_gas_usd": round(float(rec["s1_gas_usd"]), 6) if rec["s1_gas_usd"] is not None else "",
            "message_id": str(rec["message_id"] or ""),
            "bridge_amount": round(float(rec["bridge_amount"]), 8),
            "bridge_send_time": int(rec["bridge_send_time"]) if rec["bridge_send_time"] is not None else "",
            "bridge_receive_time": int(rec["bridge_receive_time"]) if rec["bridge_receive_time"] is not None else "",
            "bridge_send_tx": str(rec["bridge_send_tx"] or ""),
            "bridge_receive_tx": str(rec["bridge_receive_tx"] or ""),
            "bridge_send_explorer_uri": make_explorer_uri(src_cid, rec["bridge_send_tx"]),
            "bridge_receive_explorer_uri": make_explorer_uri(dst_cid, rec["bridge_receive_tx"]),
            "s2_timestamp": int(rec["s2_timestamp"]),
            "s2_tx_hash": str(rec["s2_tx_hash"]),
            "s2_explorer_uri": make_explorer_uri(dst_cid, rec["s2_tx_hash"]),
            "s2_actor_eoa": rec["s2_actor_eoa"] or "",
            "s2_first_called_contract": rec["s2_first_called_contract"] or "",
            "s2_beneficiary": rec["s2_beneficiary"] or "",
            "s2_amount_in": round(float(rec["s2_amount_in"]), 8),
            "s2_amount_out": round(float(rec["s2_amount_out"]), 8),
            "s2_gas_usd": round(float(rec["s2_gas_usd"]), 6) if rec["s2_gas_usd"] is not None else "",
            "leg1_bridge_mismatch": round(float(rec["leg1_bridge_mismatch"]), 6),
            "bridge_leg2_mismatch": round(float(rec["bridge_leg2_mismatch"]), 6),
            "total_cycle_latency_sec": int(rec["total_cycle_latency_sec"]),
            "gross_arbitrage_gain": round(float(rec["gross_arbitrage_gain"]), 8),
            "total_gas_origin_asset": round(float(rec["total_gas_origin_asset"]), 8),
            "net_arbitrage_gain": round(float(rec["net_arbitrage_gain"]), 8),
            "net_roi_pct": round(float(rec["net_roi_pct"]), 6) if rec["net_roi_pct"] is not None else "",
            "receipt_data_quality": str(rec["receipt_data_quality"]),
            "actor_match_summary": bool(rec["actor_match_summary"]),
            "annotator_label": "",
            "rationale_code": "",
            "evidence_notes": "",
        }
        blind_rows.append(out_row)

    for target_csv in (ANNOTATOR_1_CSV, ANNOTATOR_2_CSV):
        tmp_csv = target_csv.with_suffix(".csv.tmp")
        with open(tmp_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=BLIND_CSV_COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(blind_rows)
        if target_csv.exists():
            target_csv.unlink()
        tmp_csv.replace(target_csv)


def generate_annotation_manual() -> None:
    """Write standardized Tier B Annotation Manual to maximize inter-annotator reliability (Cohen's kappa)."""
    log.info("4. Generating standardized ANNOTATION_MANUAL.md...")
    manual_content = """# Tier B Dual-Annotation & Adjudication Manual (`cohort_8w`)

## 1. Purpose & Blinded Protocol
This benchmark evaluates **Tier B (Inventory-Based & Provenance-Incomplete Cross-Chain Arbitrage Candidates)** in accordance with Section 4.2 of the Experiment Guide and Section IV.E of the paper.

- **Sample Size**: $N = 120$ stratified candidates drawn from the provenance-incomplete candidate universe (`SEED = 42`).
- **Blinding**: Annotators are strictly blinded to `preliminary_score`, `stratum_id`, `ipw_weight`, and downstream classifier outputs.
- **Independence**: Annotator 1 (`annotator_1_blind.csv`) and Annotator 2 (`annotator_2_blind.csv`) must complete their worksheets **independently** prior to adjudication so that pre-adjudication **Cohen's $\\kappa$** can be computed without bias.

---

## 2. Allowed Values for `annotator_label`
Every row (`sample_order` 1 to 120) must be assigned **exactly one** of the following three lowercase strings in the `annotator_label` column:

1. `positive`
2. `negative`
3. `unclear`

> **CRITICAL RULE (Guide Section 4.2)**: Do **not** force `unclear` records into `negative`. Ambiguous cases where public on-chain evidence is insufficient to confirm or refute economic coordination must remain `unclear`.

---

## 3. Standardized Decision Rules (Step-by-Step)

Evaluate each candidate row in the exact order of the rules below:

### Rule A — When to Label `positive` (Inventory / Non-Atomic Cross-Chain Arbitrage)
Label a candidate `positive` **if and only if ALL four** conditions below hold:
1. **Positive Net Economic Gain After Gas**:
   - `net_arbitrage_gain > 0` (and `gross_arbitrage_gain > 0`) after accounting for DEX pool fees, bridge fees, and observable 2-leg gas costs (`total_gas_origin_asset`).
2. **Causal Timeline**:
   - `s1_timestamp <= bridge_send_time <= s2_timestamp` (and if `bridge_receive_time` is present, `bridge_send_time <= bridge_receive_time <= s2_timestamp`).
3. **Verifiable Entity or Transport Continuity across Chains**:
   At least one of the following holds:
   - **Shared Actor / Bot Infrastructure (`actor_match_summary == True`)**: The same EOA (`s1_actor_eoa == s2_actor_eoa`), same beneficiary (`s1_beneficiary == s2_beneficiary`), or same custom MEV router/executor contract (`s1_first_called_contract == s2_first_called_contract`, excluding generic public routers unless paired with matching EOA/beneficiary) executes both legs.
   - **Atomic Leg 1 Transport with Inventory/Buffered Leg 2 Execution**: `s1_tx_hash == bridge_send_tx` (Leg 1 swap atomically triggers the bridge transfer) and Leg 2 executes within the bridge latency window with tight flow (`bridge_leg2_mismatch <= 0.01`), where the slight mismatch (`0.001 < bridge_leg2_mismatch <= 0.01`) is explained by relayer fee / gas-drop or multi-hop inventory execution on the destination chain.
4. **Tight Flow Conservation**:
   - Both `leg1_bridge_mismatch <= 0.01` and `bridge_leg2_mismatch <= 0.01`.

---

### Rule B — When to Label `negative` (Coincidental Proximity / Unprofitable / Unrelated Flow)
Label a candidate `negative` if **any** of the following holds:
1. **Unprofitable Net Execution (`net_arbitrage_gain <= 0`)**:
   - Either `gross_arbitrage_gain <= 0` (price moved unfavorably / negative spread) OR `gross_arbitrage_gain > 0` but `net_arbitrage_gain <= 0` (gas fees on source and destination chains exceed gross arbitrage profit).
2. **Independent Actors with No Shared Infrastructure (`actor_match_summary == False` and `s1_tx_hash != bridge_send_tx`)**:
   - Leg 1, the bridge transfer, and Leg 2 are executed by distinct, unrelated EOAs/contracts (`s1_actor_eoa != s2_actor_eoa` and `s1_beneficiary != s2_beneficiary`), representing coincidental retail/institutional volume in the same time bucket.
3. **Incompatible Flow Residual**:
   - The swap amounts and bridge amount diverge beyond reasonable fee/slippage explanations, or on-chain explorer inspection shows the transactions belong to unrelated DeFi actions (e.g., standalone liquidity provision, lending liquidation, or retail swap).

---

### Rule C — When to Label `unclear` (Insufficient or Ambiguous Evidence)
Label a candidate `unclear` when **`net_arbitrage_gain > 0`** and the timeline is valid, **but**:
1. **Incomplete Receipt Verification (`receipt_data_quality == 'partial_receipt_verified'`) with Plausible Indirect Link**:
   - One leg is missing full receipt contract metadata, or actors use different fresh EOAs/contracts across chains (`actor_match_summary == False`) yet exhibit extremely tight amount matching (`leg1_bridge_mismatch + bridge_leg2_mismatch < 0.002`) and rapid execution (`total_cycle_latency_sec <= 60`), making it impossible to distinguish multi-wallet inventory arbitrage from coincidence using public logs alone.
2. **Shared Generic Aggregator Contract Only**:
   - `s1_first_called_contract == s2_first_called_contract` matches a public aggregator (e.g., 1inch / Uniswap Universal Router) while `s1_actor_eoa != s2_actor_eoa` and `s1_beneficiary != s2_beneficiary`, and `s1_tx_hash != bridge_send_tx`, unless token flow in explorer shows a shared vault/custodian.

---

## 4. Standardized `rationale_code` Vocabulary
To ensure structured auditing, use one of the following standardized codes in the `rationale_code` column:

| `annotator_label` | Standardized `rationale_code` | Meaning |
| :--- | :--- | :--- |
| `positive` | `pos_inventory_same_actor_net_profitable` | Same EOA/beneficiary/MEV contract across chains, tight flow, `net_arbitrage_gain > 0` |
| `positive` | `pos_atomic_leg1_buffered_leg2_net_profitable` | `s1_tx_hash == bridge_send_tx` with buffered Leg 2 fill (`mismatch <= 1%`) and `net_arbitrage_gain > 0` |
| `negative` | `neg_unprofitable_gross_or_net_after_gas` | `net_arbitrage_gain <= 0` (unprofitable before or after 2-leg gas cost) |
| `negative` | `neg_unrelated_actors_coincidental_flow` | Distinct EOAs/beneficiaries and unlinked bridge tx (`s1_tx_hash != bridge_send_tx`) |
| `negative` | `neg_flow_or_contract_incompatible` | On-chain inspection shows unrelated protocol action or incompatible flow residual |
| `unclear` | `unc_multi_wallet_tight_flow_no_direct_link` | Positive net gain & very tight flow, but distinct EOAs without deterministic custody link |
| `unclear` | `unc_shared_public_router_distinct_eoa` | Only shared address is a public router/aggregator with distinct EOAs and `net_arbitrage_gain > 0` |

---

## 5. Recording `evidence_notes`
In the `evidence_notes` column, briefly record the supporting evidence (e.g. `"Verified s1_explorer_uri and s2_explorer_uri: matching EOA 0x..., net gain +$14.20"` or `"Negative net gain (-1.85 USDC after gas)"`).
"""
    MANUAL_MD.parent.mkdir(parents=True, exist_ok=True)
    MANUAL_MD.write_text(manual_content, encoding="utf-8")


def collect_sampling_summary(
    conn: duckdb.DuckDBPyConnection,
    wall_time: float,
    peak_rss: int,
) -> dict[str, Any]:
    univ_cnt = conn.execute("SELECT COUNT(*) FROM tier_b_universe_raw;").fetchone()[0]
    samp_cnt = conn.execute("SELECT COUNT(*) FROM tier_b_master_sample;").fetchone()[0]
    strata_cnt = conn.execute("SELECT COUNT(DISTINCT stratum_id) FROM tier_b_universe_raw;").fetchone()[0]
    sampled_strata_cnt = conn.execute(
        "SELECT COUNT(DISTINCT stratum_id) FROM tier_b_master_sample;"
    ).fetchone()[0]

    ipw_stats = conn.execute(
        """
        SELECT
            SUM(ipw_weight) AS sum_ipw,
            MIN(ipw_weight) AS min_ipw,
            median(ipw_weight) AS median_ipw,
            MAX(ipw_weight) AS max_ipw,
            MIN(sampling_prob) AS min_prob,
            MAX(sampling_prob) AS max_prob
        FROM tier_b_master_sample;
        """
    ).fetchone()

    by_score_tier = conn.execute(
        """
        SELECT
            score_tier,
            COUNT(*) AS sample_n,
            SUM(ipw_weight) AS pop_est,
            AVG(preliminary_score) AS avg_score
        FROM tier_b_master_sample
        GROUP BY score_tier
        ORDER BY score_tier;
        """
    ).fetchall()

    by_bridge = conn.execute(
        """
        SELECT
            bridge_family,
            COUNT(*) AS sample_n,
            ROUND(SUM(ipw_weight), 2) AS pop_est
        FROM tier_b_master_sample
        GROUP BY bridge_family
        ORDER BY bridge_family;
        """
    ).fetchall()

    by_chain_pair = conn.execute(
        """
        SELECT
            chain_pair,
            COUNT(*) AS sample_n,
            ROUND(SUM(ipw_weight), 2) AS pop_est
        FROM tier_b_master_sample
        GROUP BY chain_pair
        ORDER BY chain_pair;
        """
    ).fetchall()

    by_delay_decile = conn.execute(
        """
        SELECT
            delay_decile,
            COUNT(*) AS sample_n
        FROM tier_b_master_sample
        GROUP BY delay_decile
        ORDER BY delay_decile;
        """
    ).fetchall()

    by_pool_liq = conn.execute(
        """
        SELECT
            pool_liquidity_tier,
            COUNT(*) AS sample_n,
            ROUND(SUM(ipw_weight), 2) AS pop_est
        FROM tier_b_master_sample
        GROUP BY pool_liquidity_tier
        ORDER BY pool_liquidity_tier;
        """
    ).fetchall()

    by_actor_rec = conn.execute(
        """
        SELECT
            actor_recurrence_bin,
            COUNT(*) AS sample_n,
            ROUND(SUM(ipw_weight), 2) AS pop_est
        FROM tier_b_master_sample
        GROUP BY actor_recurrence_bin
        ORDER BY actor_recurrence_bin;
        """
    ).fetchall()

    by_rcpt_qual = conn.execute(
        """
        SELECT
            receipt_data_quality,
            COUNT(*) AS sample_n
        FROM tier_b_master_sample
        GROUP BY receipt_data_quality
        ORDER BY receipt_data_quality;
        """
    ).fetchall()

    feature_highlights = conn.execute(
        """
        SELECT
            SUM(CASE WHEN actor_match_summary THEN 1 ELSE 0 END) AS actor_matched_cnt,
            SUM(CASE WHEN net_arbitrage_gain > 0 THEN 1 ELSE 0 END) AS positive_net_gain_cnt,
            SUM(CASE WHEN actor_match_summary AND net_arbitrage_gain > 0 THEN 1 ELSE 0 END) AS actor_matched_and_pos_net_cnt,
            SUM(CASE WHEN LOWER(s1_tx_hash) = LOWER(bridge_send_tx) THEN 1 ELSE 0 END) AS atomic_leg1_cnt
        FROM tier_b_master_sample;
        """
    ).fetchone()

    # Verify Horvitz-Thompson population sum invariant
    ipw_sum = float(ipw_stats[0])
    if abs(ipw_sum - float(univ_cnt)) > 1e-4:
        raise RuntimeError(
            f"Horvitz-Thompson IPW sum violation: sum(ipw_weight)={ipw_sum:.4f} != universe_size={univ_cnt}"
        )

    return {
        "experiment": "cohort_8w",
        "task": "step6a_tier_b_stratified_sampling",
        "status": "completed",
        "seed": SEED,
        "target_sample_size": TARGET_SAMPLE_SIZE,
        "actual_sample_size": int(samp_cnt),
        "universe_size": int(univ_cnt),
        "total_strata": int(strata_cnt),
        "sampled_strata": int(sampled_strata_cnt),
        "wall_time_seconds": wall_time,
        "peak_rss_bytes": peak_rss,
        "ipw_verification": {
            "horvitz_thompson_sum_ipw_weight": round(ipw_sum, 4),
            "exact_population_recovery": abs(ipw_sum - float(univ_cnt)) <= 1e-4,
            "min_ipw_weight": round(float(ipw_stats[1]), 4),
            "median_ipw_weight": round(float(ipw_stats[2]), 4),
            "max_ipw_weight": round(float(ipw_stats[3]), 4),
            "min_sampling_prob": round(float(ipw_stats[4]), 6),
            "max_sampling_prob": round(float(ipw_stats[5]), 6),
        },
        "sample_highlights": {
            "actor_match_summary_true": int(feature_highlights[0]),
            "positive_net_arbitrage_gain": int(feature_highlights[1]),
            "actor_match_and_positive_net_gain": int(feature_highlights[2]),
            "atomic_leg1_provenance_hard_negatives": int(feature_highlights[3]),
        },
        "stratification_breakdown": {
            "by_score_tier": {
                str(r[0]): {
                    "sample_n": int(r[1]),
                    "ipw_population_estimate": round(float(r[2]), 2),
                    "mean_preliminary_score": round(float(r[3]), 4),
                }
                for r in by_score_tier
            },
            "by_bridge_family": {
                str(r[0]): {"sample_n": int(r[1]), "ipw_population_estimate": float(r[2])}
                for r in by_bridge
            },
            "by_chain_pair": {
                str(r[0]): {"sample_n": int(r[1]), "ipw_population_estimate": float(r[2])}
                for r in by_chain_pair
            },
            "by_delay_decile": {int(r[0]): int(r[1]) for r in by_delay_decile},
            "by_pool_liquidity_tier": {
                str(r[0]): {"sample_n": int(r[1]), "ipw_population_estimate": float(r[2])}
                for r in by_pool_liq
            },
            "by_actor_recurrence_bin": {
                str(r[0]): {"sample_n": int(r[1]), "ipw_population_estimate": float(r[2])}
                for r in by_actor_rec
            },
            "by_receipt_data_quality": {str(r[0]): int(r[1]) for r in by_rcpt_qual},
        },
        "artifacts": {
            "master_sample_parquet": {
                "path": str(MASTER_PARQUET),
                "sha256": compute_file_sha256(MASTER_PARQUET),
                "size_bytes": MASTER_PARQUET.stat().st_size,
            },
            "annotator_1_blind_csv": {
                "path": str(ANNOTATOR_1_CSV),
                "sha256": compute_file_sha256(ANNOTATOR_1_CSV),
                "size_bytes": ANNOTATOR_1_CSV.stat().st_size,
                "column_count": len(BLIND_CSV_COLUMNS),
            },
            "annotator_2_blind_csv": {
                "path": str(ANNOTATOR_2_CSV),
                "sha256": compute_file_sha256(ANNOTATOR_2_CSV),
                "size_bytes": ANNOTATOR_2_CSV.stat().st_size,
                "column_count": len(BLIND_CSV_COLUMNS),
            },
            "annotation_manual_md": {
                "path": str(MANUAL_MD),
                "sha256": compute_file_sha256(MANUAL_MD),
            },
        },
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    t0 = time.time()
    TIER_B_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== STEP 6A: TIER B STRATIFIED SAMPLING (N = 120) & BLINDED WORKSHEET GENERATION ===")

    conn = duckdb.connect(str(DB_FILE))
    configure_duckdb(conn)

    try:
        ensure_labels_tier_a_view(conn)
        build_tier_b_universe(conn)
        quotas = allocate_stratum_quotas(conn)
        draw_stratified_sample(conn, quotas)
        export_blinded_csvs(conn)
        generate_annotation_manual()

        wall_time = round(time.time() - t0, 3)
        peak_rss = get_peak_rss()

        summary = collect_sampling_summary(conn, wall_time, peak_rss)
        write_json_atomic(SUMMARY_FILE, summary)

        log.info(f"=== STEP 6A COMPLETED IN {wall_time:.2f}s ===")
        log.info(
            f"Sampled {summary['actual_sample_size']} candidates across "
            f"{summary['sampled_strata']}/{summary['total_strata']} strata "
            f"(HT IPW Sum = {summary['ipw_verification']['horvitz_thompson_sum_ipw_weight']:,.1f})"
        )
        log.info(f"Score Tier Breakdown: {summary['stratification_breakdown']['by_score_tier']}")
        log.info(f"Sample Highlights: {summary['sample_highlights']}")
        log.info(f"Master Parquet : {MASTER_PARQUET}")
        log.info(f"Blinded CSV 1  : {ANNOTATOR_1_CSV}")
        log.info(f"Blinded CSV 2  : {ANNOTATOR_2_CSV}")
        log.info(f"Manual         : {MANUAL_MD}")
        log.info(f"Summary JSON   : {SUMMARY_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
