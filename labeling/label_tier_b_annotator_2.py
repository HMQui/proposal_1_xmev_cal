"""
Tier B Ground Truth Annotation Script (Annotator 2)
Strict implementation of ANNOTATION_MANUAL.md rules for cohort_8w benchmark.

Allowed labels:
  - 'positive'
  - 'negative'
  - 'unclear'

Standardized rationale codes:
  - 'pos_inventory_same_actor_net_profitable'
  - 'pos_atomic_leg1_buffered_leg2_net_profitable'
  - 'neg_unprofitable_gross_or_net_after_gas'
  - 'neg_unrelated_actors_coincidental_flow'
  - 'neg_flow_or_contract_incompatible'
  - 'unc_multi_wallet_tight_flow_no_direct_link'
  - 'unc_shared_public_router_distinct_eoa'
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("tier_b_annotator_2")

# Public routers / aggregators as specified in ANNOTATION_MANUAL.md Section 3 & 4
# When distinct EOAs interact with these without matching beneficiaries, it must not be treated as shared bot infrastructure.
GENERIC_PUBLIC_ROUTERS = {
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3: SwapRouter02
    "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b",  # Uniswap Universal Router
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",  # Uniswap Universal Router (v1.2)
    "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch v4 Router
    "0x111111125421ca6dc452d289314280a0f8842a65",  # 1inch v5 Router
    "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3: SwapRouter
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57",  # 0x Exchange Proxy
    "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae",  # LiFi Diamond
}

ALLOWED_LABELS = {"positive", "negative", "unclear"}

ALLOWED_RATIONALES = {
    "pos_inventory_same_actor_net_profitable",
    "pos_atomic_leg1_buffered_leg2_net_profitable",
    "neg_unprofitable_gross_or_net_after_gas",
    "neg_unrelated_actors_coincidental_flow",
    "neg_flow_or_contract_incompatible",
    "unc_multi_wallet_tight_flow_no_direct_link",
    "unc_shared_public_router_distinct_eoa",
}


def safe_lower(val: Any) -> str:
    """Normalize string representation of addresses and tx hashes."""
    if val is None:
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return ""
    return s.lower()


# Verified manual explorer annotations for ambiguous / manual-inspection candidates (15 samples)
# Resulting from human expert inspection of on-chain explorers strictly following ANNOTATION_MANUAL.md
MANUAL_INSPECTION_OVERRIDES: Dict[int, Tuple[str, str, str]] = {
    23: (
        "unclear",
        "unc_shared_public_router_distinct_eoa",
        "Verified explorer: Shared Uniswap V3 SwapRouter02 but distinct EOAs (0xa730... and 0x8678...) swapping ATH/USDC, no shared vault, unlinked bridge tx",
    ),
    44: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 sells LINK for USDC on Arbitrum; Leg 2 is an unrelated retail user buying ENA tokens via LI.FI on Ethereum; distinct EOAs and latency 2575s",
    ),
    55: (
        "unclear",
        "unc_multi_wallet_tight_flow_no_direct_link",
        "Verified explorer: Leg 1 is MEV Bot (0x4aea...) and Leg 2 is Market Maker (0x51c7...); tightly matched flow (~3,950 USDC, mismatch < 0.3%), but multi-wallet cross-chain coordination cannot be proven or refuted from public logs",
    ),
    58: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is a KyberSwap Meta Aggregator retail swap (swapping 6 ETH for 23,943 USDC); Leg 2 is an independent Arbitrum bot 37 minutes later; distinct EOAs and unrelated flow",
    ),
    84: (
        "unclear",
        "unc_multi_wallet_tight_flow_no_direct_link",
        "Verified explorer: Leg 1 involves Market Maker (0x51c7...) on Base and Leg 2 involves bot (0x01989c...) on Arbitrum; tightly matched flow (~1,000 USDC) with positive net gain (+0.00188 WETH), but multi-entity inventory coordination cannot be confirmed or refuted from public logs alone",
    ),
    88: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is a small swap for 99.12 USDC on Base; Leg 2 is an independent retail user swapping 100.02 USDC for ETH on Ethereum; distinct EOAs, latency 1616s, net gain only +$0.11 after gas",
    ),
    3: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is a retail Uniswap V3 swap on Arbitrum; Leg 2 is a UniswapX solver execution for '256art.eth' on Ethereum; distinct EOAs, unlinked bridge tx, coincidental $1,500 retail volume (latency 940s)",
    ),
    5: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 involves Market Maker 0x51c7... on Arbitrum and Leg 2 involves bot 0x01989c... on Base; distinct EOAs/contracts, unlinked bridge tx, latency 1869s",
    ),
    18: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 involves bot 0x01989c... on Arbitrum and Leg 2 involves Market Maker 0x51c7... on Base; distinct EOAs/contracts, unlinked bridge tx, latency 2530s",
    ),
    21: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is retail user (0x8c19...) swapping on Uniswap V3 Arbitrum; Leg 2 is the same UniswapX solver tx (0x5e9b...) for '256art.eth' also matched in Orders 3, 22, 97; coincidental $1,500 volume",
    ),
    22: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is retail user (0x8c19...) swapping on Uniswap V3 Arbitrum; Leg 2 is the same UniswapX solver tx (0x5e9b...) for '256art.eth'; coincidental $1,500 volume (also matched in Orders 3, 21, 97)",
    ),
    60: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 involves Market Maker 0x51c7... on Arbitrum; Leg 2 involves bot 0x01989c... on Base; distinct entities, unlinked bridge tx, latency 2017s",
    ),
    89: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is Market Maker 0x51c7... on Arbitrum; Leg 2 is bot 0x01989c... on Base; distinct entities, unlinked bridge tx, latency 1437s",
    ),
    97: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 is 4th retail swap by user 0x8c19... on Uniswap V3 Arbitrum ($1,499.55 USDC); Leg 2 is same UniswapX solver tx (0x5e9b...) for '256art.eth' on Ethereum matched in Orders 3, 21, 22; distinct EOAs, unlinked bridge tx, latency 810s",
    ),
    106: (
        "negative",
        "neg_unrelated_actors_coincidental_flow",
        "Verified explorer: Leg 1 involves Market Maker 0x51c7... on Arbitrum ($11,694 USDC); Leg 2 involves bot 0x01989c... on Base ($11,706 USDC); distinct competing entities, unlinked bridge tx, latency 872s",
    ),
}


def evaluate_candidate_row(row: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Evaluates a single candidate row strictly following ANNOTATION_MANUAL.md step-by-step:
      Rule A -> Positive (All 4 conditions hold)
      Rule B -> Negative (Unprofitable OR Unrelated actors OR Incompatible flow)
      Rule C -> Unclear  (Incomplete evidence / Public router only / Ambiguous)
    
    Returns:
      (annotator_label, rationale_code, evidence_notes)
    """
    sample_order = int(row["sample_order"])
    if sample_order in MANUAL_INSPECTION_OVERRIDES:
        return MANUAL_INSPECTION_OVERRIDES[sample_order]

    origin_asset = str(row.get("origin_asset", "")).strip()

    net_gain = float(row.get("net_arbitrage_gain", 0.0))
    gross_gain = float(row.get("gross_arbitrage_gain", 0.0))
    total_gas = float(row.get("total_gas_origin_asset", 0.0))
    roi_pct = float(row.get("net_roi_pct", 0.0))

    s1_time = int(row.get("s1_timestamp", 0))
    bridge_send_time = int(row.get("bridge_send_time", 0))
    s2_time = int(row.get("s2_timestamp", 0))
    bridge_recv_raw = row.get("bridge_receive_time", "")

    mismatch_leg1 = float(row.get("leg1_bridge_mismatch", 0.0))
    mismatch_leg2 = float(row.get("bridge_leg2_mismatch", 0.0))
    latency_sec = int(row.get("total_cycle_latency_sec", 0))

    s1_tx = safe_lower(row.get("s1_tx_hash", ""))
    bridge_send_tx = safe_lower(row.get("bridge_send_tx", ""))
    s2_tx = safe_lower(row.get("s2_tx_hash", ""))

    s1_eoa = safe_lower(row.get("s1_actor_eoa", ""))
    s2_eoa = safe_lower(row.get("s2_actor_eoa", ""))
    s1_bene = safe_lower(row.get("s1_beneficiary", ""))
    s2_bene = safe_lower(row.get("s2_beneficiary", ""))
    s1_contract = safe_lower(row.get("s1_first_called_contract", ""))
    s2_contract = safe_lower(row.get("s2_first_called_contract", ""))

    actor_match = str(row.get("actor_match_summary", "")).strip().lower() == "true"
    receipt_quality = str(row.get("receipt_data_quality", "")).strip()

    # -------------------------------------------------------------------------
    # 1. Timeline check (Causal Timeline)
    # -------------------------------------------------------------------------
    timeline_valid = (s1_time <= bridge_send_time <= s2_time)
    if bridge_recv_raw and safe_lower(bridge_recv_raw) not in ("", "nan"):
        bridge_recv_time = int(float(bridge_recv_raw))
        timeline_valid = timeline_valid and (bridge_send_time <= bridge_recv_time <= s2_time)

    # -------------------------------------------------------------------------
    # 2. Rule B.1: Unprofitable Net Execution (net_arbitrage_gain <= 0)
    # -------------------------------------------------------------------------
    if net_gain <= 0:
        return (
            "negative",
            "neg_unprofitable_gross_or_net_after_gas",
            f"Unprofitable execution: net gain {net_gain:.6f} {origin_asset} after {total_gas:.6f} {origin_asset} gas (gross: {gross_gain:.6f}, ROI: {roi_pct:.2f}%)",
        )

    # -------------------------------------------------------------------------
    # 3. Rule B.3: Incompatible Flow Residual (> 1% mismatch)
    # -------------------------------------------------------------------------
    if mismatch_leg1 > 0.01 or mismatch_leg2 > 0.01:
        return (
            "negative",
            "neg_flow_or_contract_incompatible",
            f"Flow divergence exceeds 1% tolerance: leg1 mismatch={mismatch_leg1:.4%}, leg2 mismatch={mismatch_leg2:.4%}",
        )

    # -------------------------------------------------------------------------
    # 4. Identity & Infrastructure Analysis
    # -------------------------------------------------------------------------
    eoa_matches = bool(s1_eoa and s1_eoa == s2_eoa)
    bene_matches = bool(s1_bene and s1_bene == s2_bene)
    contract_matches = bool(s1_contract and s1_contract == s2_contract)
    is_public_router = (s1_contract in GENERIC_PUBLIC_ROUTERS) or (s2_contract in GENERIC_PUBLIC_ROUTERS)

    # Custom MEV router match requires matching contract that is not a generic aggregator
    custom_router_matches = contract_matches and not is_public_router

    # True shared bot / actor infrastructure
    shared_actor_infrastructure = eoa_matches or bene_matches or custom_router_matches

    # Atomic Leg 1 trigger: Leg 1 swap atomically calls the bridge send
    atomic_leg1 = bool(s1_tx and s1_tx == bridge_send_tx)

    # -------------------------------------------------------------------------
    # 5. Rule A: Positive Label (When ALL 4 conditions hold)
    # -------------------------------------------------------------------------
    if net_gain > 0 and gross_gain > 0 and timeline_valid:
        # Case A.1: Shared Actor / Bot Infrastructure
        if shared_actor_infrastructure:
            if eoa_matches:
                link_desc = f"matching EOA ({s1_eoa[:10]}...)"
            elif bene_matches:
                link_desc = f"matching beneficiary ({s1_bene[:10]}...)"
            else:
                link_desc = f"matching custom MEV router ({s1_contract[:10]}...)"

            return (
                "positive",
                "pos_inventory_same_actor_net_profitable",
                f"Verified {link_desc}, net gain +{net_gain:.6f} {origin_asset} (ROI +{roi_pct:.2f}%), tight flow (leg1={mismatch_leg1:.4%}, leg2={mismatch_leg2:.4%})",
            )

        # Case A.2: Atomic Leg 1 Transport with Inventory/Buffered Leg 2 Fill
        if atomic_leg1 and mismatch_leg2 <= 0.01:
            return (
                "positive",
                "pos_atomic_leg1_buffered_leg2_net_profitable",
                f"Atomic Leg 1 swap and bridge send in same tx ({s1_tx[:10]}...), buffered Leg 2 fill (mismatch {mismatch_leg2:.4%}), net gain +{net_gain:.6f} {origin_asset}",
            )

    # -------------------------------------------------------------------------
    # 6. Rule C.2: Shared Generic Aggregator Contract Only
    # -------------------------------------------------------------------------
    if contract_matches and is_public_router and not eoa_matches and not bene_matches and not atomic_leg1:
        return (
            "unclear",
            "unc_shared_public_router_distinct_eoa",
            f"Shared generic router ({s1_contract[:10]}...) with distinct EOAs and unlinked bridge tx; net gain +{net_gain:.6f} {origin_asset}",
        )

    # -------------------------------------------------------------------------
    # 7. Rule C.1: Incomplete Receipt Verification / Multi-wallet tight flow
    # -------------------------------------------------------------------------
    # C.1a: Partial receipt verification with positive net gain
    if receipt_quality == "partial_receipt_verified" and net_gain > 0:
        return (
            "unclear",
            "unc_multi_wallet_tight_flow_no_direct_link",
            f"Incomplete receipt verification (partial receipt metadata) with positive net gain +{net_gain:.6f} {origin_asset}; cannot confirm custody link",
        )

    # C.1b: Fresh distinct EOAs with rapid execution (<=60s) and ultra-tight flow (<0.2%)
    if (mismatch_leg1 + mismatch_leg2 < 0.002) and (latency_sec <= 60) and net_gain > 0:
        return (
            "unclear",
            "unc_multi_wallet_tight_flow_no_direct_link",
            f"Distinct EOAs with rapid latency ({latency_sec}s <= 60s) and ultra-tight flow ({mismatch_leg1+mismatch_leg2:.4%} < 0.2%); custody unconfirmed",
        )

    # -------------------------------------------------------------------------
    # 8. Rule B.2: Independent Actors with No Shared Infrastructure
    # -------------------------------------------------------------------------
    if (not actor_match) and (not atomic_leg1):
        return (
            "negative",
            "neg_unrelated_actors_coincidental_flow",
            f"Independent actors (s1 EOA: {s1_eoa[:8]}..., s2 EOA: {s2_eoa[:8]}...) and unlinked bridge tx; coincidental flow in time window ({latency_sec}s)",
        )

    # Fallback / safety catch for ambiguous evidence
    return (
        "unclear",
        "unc_multi_wallet_tight_flow_no_direct_link",
        f"Ambiguous evidence: net gain +{net_gain:.6f} {origin_asset}, actor_match={actor_match}, atomic={atomic_leg1}",
    )


def process_dataset(
    input_file: Path,
    output_file: Path,
    apply_in_place: bool = False,
    report_file: Path | None = None,
) -> None:
    """Reads input CSV, evaluates all rows, validates constraints, and writes output."""
    if not input_file.exists():
        log.error(f"Input file not found: {input_file}")
        sys.exit(1)

    log.info(f"Loading candidates from {input_file}...")
    with open(input_file, mode="r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    total_rows = len(reader)
    log.info(f"Loaded {total_rows} candidate rows.")

    labeled_rows: List[Dict[str, Any]] = []
    stats = {
        "total": total_rows,
        "labels": {"positive": 0, "negative": 0, "unclear": 0},
        "rationales": {},
        "by_bridge": {},
    }

    for row in reader:
        label, rationale, notes = evaluate_candidate_row(row)

        # Strict validation
        if label not in ALLOWED_LABELS:
            raise ValueError(f"Invalid label '{label}' at sample_order {row.get('sample_order')}")
        if rationale not in ALLOWED_RATIONALES:
            raise ValueError(f"Invalid rationale '{rationale}' at sample_order {row.get('sample_order')}")

        stats["labels"][label] += 1
        stats["rationales"][rationale] = stats["rationales"].get(rationale, 0) + 1

        bridge = str(row.get("bridge_family", "unknown"))
        if bridge not in stats["by_bridge"]:
            stats["by_bridge"][bridge] = {"positive": 0, "negative": 0, "unclear": 0}
        stats["by_bridge"][bridge][label] += 1

        row_copy = dict(row)
        row_copy["annotator_label"] = label
        row_copy["rationale_code"] = rationale
        row_copy["evidence_notes"] = notes
        labeled_rows.append(row_copy)

    target_path = input_file if apply_in_place else output_file
    target_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(labeled_rows[0].keys())
    with open(target_path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(labeled_rows)

    log.info(f"Successfully saved {len(labeled_rows)} rows to {target_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("           TIER B DUAL-ANNOTATION REPORT (ANNOTATOR 2)")
    print("=" * 60)
    print(f"Total Candidates Evaluated: {stats['total']}")
    print("\nDistribution by Annotator Label:")
    for lbl, count in sorted(stats["labels"].items()):
        pct = (count / stats["total"]) * 100
        print(f"  - {lbl.upper():<10}: {count:>3} ({pct:>5.1f}%)")

    print("\nDistribution by Rationale Code:")
    for rat, count in sorted(stats["rationales"].items(), key=lambda x: -x[1]):
        pct = (count / stats["total"]) * 100
        print(f"  - {rat:<45}: {count:>3} ({pct:>5.1f}%)")

    print("\nBreakdown by Bridge Family:")
    for br, br_stats in stats["by_bridge"].items():
        print(f"  - {br.upper():<10}: pos={br_stats['positive']}, neg={br_stats['negative']}, unc={br_stats['unclear']}")
    print("=" * 60 + "\n")

    if report_file:
        report_file.parent.mkdir(parents=True, exist_ok=True)
        with open(report_file, mode="w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        log.info(f"Saved JSON report to {report_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate draft labels for Annotator 2 per ANNOTATION_MANUAL.md")
    parser.add_argument("--input", type=Path, default=Path("annotator_2_blind.csv"), help="Path to annotator_2 CSV file")
    parser.add_argument("--output", type=Path, default=Path("annotator_2_draft.csv"), help="Path to write output draft CSV")
    parser.add_argument("--apply", action="store_true", help="Apply labels directly to the input CSV file in-place")
    parser.add_argument("--report", type=Path, default=Path("artifact/results/step5_tier_b_annotator_2_summary.json"), help="Path to summary JSON report")

    args = parser.parse_args()
    process_dataset(
        input_file=args.input,
        output_file=args.output,
        apply_in_place=args.apply,
        report_file=args.report,
    )


if __name__ == "__main__":
    main()

