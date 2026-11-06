"""배치마다 돌리는 데이터 품질 검사. 결과는 dq_results 테이블에 쌓는다."""
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .schema import TX_TYPES, table


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    check_name: str
    metric: float
    threshold: float
    passed: bool


def run_checks(
    spark: SparkSession,
    ns: str,
    from_day: int,
    to_day: int,
    duplicate_rows: int,
    max_quarantine_ratio: float,
) -> list[CheckResult]:
    in_days = F.col("sim_day").between(from_day, to_day)
    in_days_or_unknown = in_days | (F.col("sim_day") == 0)

    bronze_rows = spark.table(table(ns, "bronze_transactions")).where(in_days_or_unknown).count()
    silver = spark.table(table(ns, "silver_transactions")).where(in_days).cache()
    silver_rows = silver.count()
    quarantine_rows = spark.table(table(ns, "silver_quarantine")).where(in_days_or_unknown).count()
    gold_rows = (
        spark.table(table(ns, "gold_daily_type_summary"))
        .where(in_days)
        .agg(F.coalesce(F.sum("tx_count"), F.lit(0)))
        .first()[0]
    )

    silver_stats = silver.agg(
        F.countDistinct("tx_id").alias("distinct_ids"),
        F.sum((F.col("orig_account").isNull() | F.col("dest_account").isNull() | F.col("amount").isNull()).cast("long")).alias("null_keys"),
        F.sum((F.col("amount") <= 0).cast("long")).alias("non_positive"),
        F.sum((~F.col("tx_type").isin(*TX_TYPES)).cast("long")).alias("unknown_type"),
    ).first()
    silver.unpersist()

    def zero(name: str, value) -> CheckResult:
        value = float(value or 0)
        return CheckResult(name, value, 0.0, value == 0)

    quarantine_ratio = quarantine_rows / bronze_rows if bronze_rows else 0.0
    return [
        # bronze 행 = silver 행 + 격리 행 + 중복으로 빠진 행
        zero("row_reconciliation_diff", bronze_rows - (silver_rows + quarantine_rows + duplicate_rows)),
        zero("silver_duplicate_tx_id", silver_rows - silver_stats["distinct_ids"]),
        zero("silver_null_required", silver_stats["null_keys"]),
        zero("silver_non_positive_amount", silver_stats["non_positive"]),
        zero("silver_unknown_tx_type", silver_stats["unknown_type"]),
        zero("gold_vs_silver_count_diff", gold_rows - silver_rows),
        CheckResult("quarantine_ratio", quarantine_ratio, max_quarantine_ratio, quarantine_ratio <= max_quarantine_ratio),
    ]


def save(spark: SparkSession, ns: str, batch_id: str, results: list[CheckResult]) -> None:
    rows = [(batch_id, r.check_name, r.metric, r.threshold, r.passed) for r in results]
    df = spark.createDataFrame(
        rows, "batch_id string, check_name string, metric double, threshold double, passed boolean"
    ).withColumn("checked_at", F.current_timestamp())
    df.writeTo(table(ns, "dq_results")).append()
