"""배치 실행 진입점.

예) 1~7일차를 적재
    python -m paysim_pipeline.run --source data/raw/paysim.csv --from-day 1 --to-day 7
"""
import argparse
import json
import os
import time
from dataclasses import asdict

from pyspark.sql import SparkSession

from . import bronze, gold, quality, silver, tables
from .spark import build_spark


def run_batch(
    spark: SparkSession,
    source: str,
    ns: str,
    from_day: int,
    to_day: int,
    max_quarantine_ratio: float = 0.01,
    fail_on_dq: bool = True,
) -> dict:
    if from_day < 1 or to_day < from_day:
        raise ValueError("from_day >= 1 이고 to_day >= from_day 여야 합니다.")
    started = time.time()
    batch_id = f"days_{from_day:03d}_{to_day:03d}"

    tables.create_all(spark, ns)

    raw = bronze.read_raw(spark, source)
    bronze.write(bronze.prepare(raw, os.path.basename(source), batch_id, from_day, to_day), ns)

    stats = silver.load(spark, ns, batch_id, from_day, to_day, bronze.read_batch(spark, ns, from_day, to_day))
    gold.refresh(spark, ns, from_day, to_day)

    results = quality.run_checks(spark, ns, from_day, to_day, stats.duplicate_rows, max_quarantine_ratio)
    quality.save(spark, ns, batch_id, results)

    summary = {
        "batch_id": batch_id,
        "silver": asdict(stats),
        "checks": [asdict(r) for r in results],
        "seconds": round(time.time() - started, 1),
    }
    failed = [r.check_name for r in results if not r.passed]
    if failed and fail_on_dq:
        raise quality.DataQualityError(f"품질 검사 실패: {failed}\n{json.dumps(summary, ensure_ascii=False, indent=2)}")
    return summary


def main() -> None:
    p = argparse.ArgumentParser(description="PaySim → Iceberg 배치 적재")
    p.add_argument("--source", required=True, help="PaySim CSV 경로")
    p.add_argument("--warehouse", default="warehouse", help="Iceberg 웨어하우스 디렉터리")
    p.add_argument("--namespace", default="paysim")
    p.add_argument("--from-day", type=int, required=True)
    p.add_argument("--to-day", type=int, required=True)
    p.add_argument("--max-quarantine-ratio", type=float, default=0.01)
    p.add_argument("--output", help="결과 JSON을 저장할 경로 (stdout에는 Spark 로그가 섞이므로 파일로 따로 남긴다)")
    args = p.parse_args()

    spark = build_spark(args.warehouse)
    try:
        summary = run_batch(spark, args.source, args.namespace, args.from_day, args.to_day, args.max_quarantine_ratio)
        text = json.dumps(summary, ensure_ascii=False, indent=2)
        print(text)
        if args.output:
            os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(text + "\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
