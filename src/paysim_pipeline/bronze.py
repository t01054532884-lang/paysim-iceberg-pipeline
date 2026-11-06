"""원본 CSV → bronze_transactions.

- 모든 값을 문자열로 읽어 원본을 그대로 보관한다(형 변환은 silver에서).
- 행 내용 전체의 해시를 tx_id로 쓴다. PaySim에는 거래 ID가 없기 때문이다.
- sim_day 파티션 단위로 덮어써서, 같은 배치를 다시 돌려도 중복이 생기지 않는다.
- step을 해석할 수 없는 행은 sim_day=0 파티션에 모아 silver 단계에서 격리한다.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schema import HOURS_PER_DAY, RAW_COLUMNS, table


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    missing = set(RAW_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"원본 CSV에 칼럼이 없습니다: {sorted(missing)}")
    return df.select(*RAW_COLUMNS)


def sim_day_expr() -> F.Column:
    step = F.col("step").cast("int")
    day = (F.floor((step - 1) / HOURS_PER_DAY) + 1).cast("int")
    return F.coalesce(day, F.lit(0))


def row_hash_expr() -> F.Column:
    parts = [F.coalesce(F.col(c), F.lit("\\N")) for c in RAW_COLUMNS]
    return F.sha2(F.concat_ws("|", *parts), 256)


def prepare(raw: DataFrame, source_file: str, batch_id: str, from_day: int, to_day: int) -> DataFrame:
    df = raw.withColumn("sim_day", sim_day_expr())
    df = df.where(F.col("sim_day").between(from_day, to_day) | (F.col("sim_day") == 0))
    return df.select(
        *RAW_COLUMNS,
        "sim_day",
        row_hash_expr().alias("_row_hash"),
        F.lit(source_file).alias("_source_file"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
    )


def write(df: DataFrame, ns: str) -> None:
    df.writeTo(table(ns, "bronze_transactions")).overwritePartitions()


def read_batch(spark: SparkSession, ns: str, from_day: int, to_day: int) -> DataFrame:
    return spark.table(table(ns, "bronze_transactions")).where(
        F.col("sim_day").between(from_day, to_day) | (F.col("sim_day") == 0)
    )
