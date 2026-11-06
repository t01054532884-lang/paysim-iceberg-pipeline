"""bronze → silver_transactions / silver_quarantine.

- 형 변환에 실패하거나 규칙에 어긋나는 행은 버리지 않고 사유와 함께 격리한다.
- 같은 내용의 행(=같은 tx_id)은 한 건만 남긴다.
- silver에는 MERGE INTO ... WHEN NOT MATCHED THEN INSERT 로 넣는다.
  tx_id가 행 내용의 해시이므로 이미 있는 tx_id는 내용도 같고, 다시 돌리면 새로 들어가는 행이 0건이다.
"""
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schema import (
    HOURS_PER_DAY,
    MONEY,
    QUARANTINE_COLUMNS,
    RAW_COLUMNS,
    SILVER_COLUMNS,
    TX_TYPES,
    table,
)

BALANCE_COLUMNS = ["orig_balance_before", "orig_balance_after", "dest_balance_before", "dest_balance_after"]


@dataclass
class SilverLoadStats:
    valid_rows: int
    duplicate_rows: int
    quarantined_rows: int


def _typed(bronze: DataFrame) -> DataFrame:
    step = F.col("step").cast("int")
    return bronze.select(
        *RAW_COLUMNS,
        F.col("_row_hash").alias("tx_id"),
        "sim_day",
        step.alias("_step"),
        F.upper(F.trim("type")).alias("tx_type"),
        F.col("amount").cast(MONEY).alias("_amount"),
        F.trim("nameOrig").alias("orig_account"),
        F.col("oldbalanceOrg").cast(MONEY).alias("orig_balance_before"),
        F.col("newbalanceOrig").cast(MONEY).alias("orig_balance_after"),
        F.trim("nameDest").alias("dest_account"),
        F.col("oldbalanceDest").cast(MONEY).alias("dest_balance_before"),
        F.col("newbalanceDest").cast(MONEY).alias("dest_balance_after"),
        F.col("isFraud").cast("int").alias("_is_fraud"),
        F.col("isFlaggedFraud").cast("int").alias("_is_flagged"),
    )


def _reason() -> F.Column:
    """첫 번째로 걸린 사유 하나만 남긴다. 모두 통과하면 null."""
    any_balance_null = None
    for c in BALANCE_COLUMNS:
        cond = F.col(c).isNull()
        any_balance_null = cond if any_balance_null is None else (any_balance_null | cond)
    blank = lambda c: F.col(c).isNull() | (F.col(c) == "")  # noqa: E731
    return (
        F.when(F.col("_step").isNull() | (F.col("_step") < 1), "invalid_step")
        .when(~F.col("tx_type").isin(*TX_TYPES) | F.col("tx_type").isNull(), "unknown_type")
        .when(F.col("_amount").isNull(), "invalid_amount")
        .when(F.col("_amount") <= 0, "non_positive_amount")
        .when(any_balance_null, "invalid_balance")
        .when(blank("orig_account") | blank("dest_account"), "missing_account")
        .when(
            ~F.col("_is_fraud").isin(0, 1) | F.col("_is_fraud").isNull()
            | ~F.col("_is_flagged").isin(0, 1) | F.col("_is_flagged").isNull(),
            "invalid_label",
        )
    )


def split(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(정상 행, 격리 행). 정상 행은 아직 중복 제거 전이다."""
    checked = _typed(bronze).withColumn("reason", _reason())

    debit_like_gap = F.col("orig_balance_before") - F.col("_amount") - F.col("orig_balance_after")
    cash_in_gap = F.col("orig_balance_before") + F.col("_amount") - F.col("orig_balance_after")

    valid = checked.where(F.col("reason").isNull()).select(
        "tx_id",
        "sim_day",
        F.col("_step").alias("step"),
        ((F.col("_step") - 1) % HOURS_PER_DAY).alias("hour_of_day"),
        "tx_type",
        F.col("_amount").alias("amount"),
        "orig_account",
        "orig_balance_before",
        "orig_balance_after",
        "dest_account",
        "dest_balance_before",
        "dest_balance_after",
        F.col("dest_account").startswith("M").alias("dest_is_merchant"),
        # 거래 전후 잔액이 금액만큼 정확히 움직였는지. 0이 아니면 원본 잔액 기록이 어긋난 거래
        F.when(F.col("tx_type") == "CASH_IN", cash_in_gap).otherwise(debit_like_gap).cast(MONEY).alias("orig_balance_gap"),
        (F.col("_is_fraud") == 1).alias("is_fraud"),
        (F.col("_is_flagged") == 1).alias("is_flagged_fraud"),
    )
    quarantine = checked.where(F.col("reason").isNotNull()).select(*RAW_COLUMNS, "tx_id", "sim_day", "reason")
    return valid, quarantine


def load(spark: SparkSession, ns: str, batch_id: str, from_day: int, to_day: int, bronze: DataFrame) -> SilverLoadStats:
    valid, quarantine = split(bronze)
    valid = valid.cache()
    valid_rows = valid.count()

    deduped = (
        valid.dropDuplicates(["tx_id"])
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_loaded_at", F.current_timestamp())
        .select(*SILVER_COLUMNS)
    )
    deduped_rows = deduped.count()

    deduped.createOrReplaceTempView("silver_batch_src")
    spark.sql(f"""
        MERGE INTO {table(ns, 'silver_transactions')} t
        USING silver_batch_src s
        ON t.tx_id = s.tx_id AND t.sim_day BETWEEN {from_day} AND {to_day}
        WHEN NOT MATCHED THEN INSERT *
    """)

    quarantine = (
        quarantine.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_quarantined_at", F.current_timestamp())
        .select(*QUARANTINE_COLUMNS)
        .cache()
    )
    quarantined_rows = quarantine.count()
    if quarantined_rows:
        quarantine.writeTo(table(ns, "silver_quarantine")).overwritePartitions()

    valid.unpersist()
    quarantine.unpersist()
    return SilverLoadStats(
        valid_rows=deduped_rows,
        duplicate_rows=valid_rows - deduped_rows,
        quarantined_rows=quarantined_rows,
    )
