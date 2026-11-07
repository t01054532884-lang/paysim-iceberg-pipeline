"""적재가 끝난 테이블에서 README '결과'에 옮길 숫자를 뽑아 docs/results.md에 남긴다.

테이블을 읽기만 하고 바꾸지 않는다.

1. 테이블별 행 수와 행 대조 (bronze = silver + 격리 + 중복)
2. 격리 사유별 행 수
3. 원본 잔액 기록이 어긋난 거래 수 (orig_balance_gap != 0)
4. 탐지 규칙 비교 (gold_rule_evaluation)
"""
import argparse
import os

from pyspark.sql import SparkSession

from .iceberg_demo import scalar, to_markdown
from .schema import table
from .spark import build_spark


def run(spark: SparkSession, ns: str, out_path: str) -> str:
    bronze = table(ns, "bronze_transactions")
    silver = table(ns, "silver_transactions")
    quarantine = table(ns, "silver_quarantine")
    out: list[str] = ["# 적재 결과", "", "`python -m paysim_pipeline.report` 실행 출력입니다.", ""]

    bronze_rows = scalar(spark, f"SELECT count(*) FROM {bronze}")
    silver_rows = scalar(spark, f"SELECT count(*) FROM {silver}")
    quarantine_rows = scalar(spark, f"SELECT count(*) FROM {quarantine}")
    duplicate_rows = bronze_rows - silver_rows - quarantine_rows
    out += [
        "## 1. 행 수",
        "",
        "| 테이블 | 행 수 |",
        "|---|---|",
        f"| bronze_transactions (원본) | {bronze_rows:,} |",
        f"| silver_transactions | {silver_rows:,} |",
        f"| silver_quarantine | {quarantine_rows:,} |",
        f"| 중복 (bronze − silver − 격리) | {duplicate_rows:,} |",
        "",
    ]

    reasons = spark.sql(f"""
        SELECT reason, type AS raw_type, count(*) AS rows
        FROM {quarantine} GROUP BY reason, type ORDER BY rows DESC, reason
    """)
    out += ["## 2. 격리 사유", "", to_markdown(reasons, 100), ""]

    gap = spark.sql(f"""
        SELECT tx_type,
               count(*) AS tx_count,
               sum(CASE WHEN orig_balance_gap != 0 THEN 1 ELSE 0 END) AS gap_count
        FROM {silver} GROUP BY tx_type ORDER BY tx_type
    """)
    out += [
        "## 3. 원본 잔액 기록이 어긋난 거래",
        "",
        "`orig_balance_gap` = 거래 전 잔액 − 금액 − 거래 후 잔액 (CASH_IN은 + 금액). 0이 아니면 잔액 기록이 금액만큼 움직이지 않은 거래입니다.",
        "",
        to_markdown(gap),
        "",
    ]

    rules = spark.sql(f"""
        SELECT rule_name, tp, fp, fn, round(precision, 4) AS precision, round(recall, 4) AS recall
        FROM {table(ns, 'gold_rule_evaluation')} ORDER BY rule_name
    """)
    out += ["## 4. 탐지 규칙 비교 (전체 silver 기준)", "", to_markdown(rules), ""]

    text = "\n".join(out)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    return text


def main() -> None:
    p = argparse.ArgumentParser(description="적재 결과 숫자를 마크다운으로 저장")
    p.add_argument("--warehouse", default="warehouse")
    p.add_argument("--namespace", default="paysim")
    p.add_argument("--out", default="docs/results.md")
    args = p.parse_args()
    spark = build_spark(args.warehouse)
    try:
        print(run(spark, args.namespace, args.out))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
