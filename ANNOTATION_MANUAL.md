# Tier B Dual-Annotation & Adjudication Manual (`cohort_8w`)

## 1. Purpose & Blinded Protocol
This benchmark evaluates **Tier B (Inventory-Based & Provenance-Incomplete Cross-Chain Arbitrage Candidates)** in accordance with Section 4.2 of the Experiment Guide and Section IV.E of the paper.

- **Sample Size**: $N = 120$ stratified candidates drawn from the provenance-incomplete candidate universe (`SEED = 42`).
- **Blinding**: Annotators are strictly blinded to `preliminary_score`, `stratum_id`, `ipw_weight`, and downstream classifier outputs.
- **Independence**: Annotator 1 (`annotator_1_blind.csv`) and Annotator 2 (`annotator_2_blind.csv`) must complete their worksheets **independently** prior to adjudication so that pre-adjudication **Cohen's $\kappa$** can be computed without bias.

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
