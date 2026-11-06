from decimal import Decimal

from pyspark.sql import functions as F

from paysim_pipeline import bronze, silver
from paysim_pipeline.run import run_batch
from paysim_pipeline.schema import table

# 테스트 데이터는 15행 중 4행이 격리 대상이라 비율 기준을 넉넉히 둔다.
MAX_Q = 0.5


def count(spark, ns, name, where="true"):
    return spark.table(table(ns, name)).where(where).count()


def test_split_valid_and_quarantine(spark, sample_csv):
    raw = bronze.read_raw(spark, sample_csv)
    prepared = bronze.prepare(raw, "sample.csv", "b", 1, 3)
    valid, quarantine = silver.split(prepared)

    assert valid.count() == 11  # 중복 1건 포함
    assert valid.dropDuplicates(["tx_id"]).count() == 10
    reasons = {r["reason"]: r["count"] for r in quarantine.groupBy("reason").count().collect()}
    assert reasons == {
        "non_positive_amount": 1,
        "unknown_type": 1,
        "invalid_balance": 1,
        "invalid_step": 1,
    }


def test_balance_gap_and_flags(spark, sample_csv):
    raw = bronze.read_raw(spark, sample_csv)
    valid, _ = silver.split(bronze.prepare(raw, "sample.csv", "b", 1, 3))
    rows = {r["orig_account"]: r for r in valid.collect()}

    assert rows["C100"]["orig_balance_gap"] == Decimal("0.00")  # 5000 - 1000 - 4000
    assert rows["C103"]["orig_balance_gap"] == Decimal("0.00")  # CASH_IN: 100 + 500 - 600
    assert rows["C100"]["dest_is_merchant"] is True
    assert rows["C105"]["is_flagged_fraud"] is True
    assert rows["C105"]["sim_day"] == 2
    assert rows["C105"]["hour_of_day"] == 0


def test_incremental_batches(spark, sample_csv, ns):
    s1 = run_batch(spark, sample_csv, ns, 1, 1, max_quarantine_ratio=MAX_Q)
    assert s1["silver"] == {"valid_rows": 5, "duplicate_rows": 1, "quarantined_rows": 2}
    assert count(spark, ns, "silver_transactions") == 5

    run_batch(spark, sample_csv, ns, 2, 3, max_quarantine_ratio=MAX_Q)
    assert count(spark, ns, "silver_transactions") == 10
    assert count(spark, ns, "bronze_transactions") == 15
    assert count(spark, ns, "silver_quarantine") == 4


def test_rerun_is_idempotent(spark, sample_csv, ns):
    run_batch(spark, sample_csv, ns, 1, 3, max_quarantine_ratio=MAX_Q)
    first = {n: count(spark, ns, n) for n in ["bronze_transactions", "silver_transactions", "silver_quarantine"]}

    summary = run_batch(spark, sample_csv, ns, 1, 3, max_quarantine_ratio=MAX_Q)
    second = {n: count(spark, ns, n) for n in ["bronze_transactions", "silver_transactions", "silver_quarantine"]}

    assert first == second == {"bronze_transactions": 15, "silver_transactions": 10, "silver_quarantine": 4}
    assert all(c["passed"] for c in summary["checks"])


def test_gold_tables(spark, sample_csv, ns):
    run_batch(spark, sample_csv, ns, 1, 3, max_quarantine_ratio=MAX_Q)

    day1 = spark.table(table(ns, "gold_daily_type_summary")).where("sim_day = 1")
    assert day1.agg(F.sum("tx_count")).first()[0] == 5
    assert day1.agg(F.sum("total_amount")).first()[0] == Decimal("7700.00")
    assert day1.agg(F.sum("fraud_count")).first()[0] == 2

    rules = {r["rule_name"]: r for r in spark.table(table(ns, "gold_rule_evaluation")).collect()}
    assert (rules["paysim_flag"]["tp"], rules["paysim_flag"]["fn"]) == (1, 2)
    drain = rules["full_balance_drain"]
    assert (drain["tp"], drain["fp"], drain["fn"], drain["tn"]) == (3, 1, 0, 6)
    assert drain["precision"] == 0.75


def test_quality_check_fails_on_high_quarantine_ratio(spark, sample_csv, ns):
    summary = run_batch(spark, sample_csv, ns, 1, 3, max_quarantine_ratio=0.01, fail_on_dq=False)
    by_name = {c["check_name"]: c for c in summary["checks"]}
    assert by_name["quarantine_ratio"]["passed"] is False
    assert by_name["row_reconciliation_diff"]["passed"] is True
