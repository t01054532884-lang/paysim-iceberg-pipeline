"""silver → gold.

- gold_daily_type_summary: 시뮬레이션 일자 × 거래 유형별 건수·금액·사기 건수 (배치 일자 파티션만 다시 계산)
- gold_rule_evaluation: 단순 탐지 규칙이 실제 사기 라벨을 얼마나 잡는지 (전체 silver 기준으로 다시 계산)
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schema import MONEY, MONEY_SUM, table


def daily_type_summary(silver: DataFrame) -> DataFrame:
    return silver.groupBy("sim_day", "tx_type").agg(
        F.count("*").alias("tx_count"),
        F.sum("amount").cast(MONEY_SUM).alias("total_amount"),
        F.avg("amount").cast(MONEY).alias("avg_amount"),
        F.sum(F.col("is_fraud").cast("long")).alias("fraud_count"),
        F.coalesce(F.sum(F.when(F.col("is_fraud"), F.col("amount"))), F.lit(0)).cast(MONEY_SUM).alias("fraud_amount"),
        F.sum(F.col("is_flagged_fraud").cast("long")).alias("flagged_count"),
    )


def rules() -> dict[str, F.Column]:
    drain_types = F.col("tx_type").isin("TRANSFER", "CASH_OUT")
    return {
        # 원본 데이터에 들어 있는 플래그(시뮬레이터가 단일 송금 200,000 초과 시도에 붙임)
        "paysim_flag": F.col("is_flagged_fraud"),
        # 출금·이체로 보낸 계좌 잔액을 한 번에 0으로 비운 거래
        "full_balance_drain": drain_types
        & (F.col("orig_balance_before") > 0)
        & (F.col("orig_balance_after") == 0)
        & (F.col("amount") == F.col("orig_balance_before")),
    }


def rule_evaluation(silver: DataFrame) -> DataFrame:
    frames = []
    for name, cond in rules().items():
        hit = F.coalesce(cond, F.lit(False))
        frames.append(
            silver.agg(
                F.coalesce(F.sum((hit & F.col("is_fraud")).cast("long")), F.lit(0)).alias("tp"),
                F.coalesce(F.sum((hit & ~F.col("is_fraud")).cast("long")), F.lit(0)).alias("fp"),
                F.coalesce(F.sum((~hit & F.col("is_fraud")).cast("long")), F.lit(0)).alias("fn"),
                F.coalesce(F.sum((~hit & ~F.col("is_fraud")).cast("long")), F.lit(0)).alias("tn"),
            ).withColumn("rule_name", F.lit(name))
        )
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)
    return df.select(
        "rule_name", "tp", "fp", "fn", "tn",
        F.when(F.col("tp") + F.col("fp") > 0, F.col("tp") / (F.col("tp") + F.col("fp"))).alias("precision"),
        F.when(F.col("tp") + F.col("fn") > 0, F.col("tp") / (F.col("tp") + F.col("fn"))).alias("recall"),
        F.current_timestamp().alias("evaluated_at"),
    )


def refresh(spark: SparkSession, ns: str, from_day: int, to_day: int) -> None:
    silver = spark.table(table(ns, "silver_transactions"))
    batch = silver.where(F.col("sim_day").between(from_day, to_day))
    daily_type_summary(batch).writeTo(table(ns, "gold_daily_type_summary")).overwritePartitions()
    rule_evaluation(silver).writeTo(table(ns, "gold_rule_evaluation")).overwrite(F.lit(True))
