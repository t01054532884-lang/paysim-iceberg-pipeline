"""Iceberg 테이블 유지보수.

배치를 여러 번 돌리면 파티션마다 작은 데이터 파일과 스냅샷이 계속 쌓인다.
- rewrite_data_files: 파티션 안의 작은 파일을 큰 파일로 합친다 (compaction)
- rewrite_manifests: 파일 목록(manifest)을 다시 묶어 계획 단계를 가볍게 한다
- expire_snapshots: 오래된 스냅샷과 그 스냅샷만 참조하던 파일을 지운다

전후의 파일 수·스냅샷 수·행 수를 비교해 docs/maintenance.md에 남긴다. 행 수가 달라지면 실패로 본다.
"""
import argparse
import os
from dataclasses import dataclass

from pyspark.sql import SparkSession

from .iceberg_demo import scalar
from .schema import table
from .spark import build_spark

TABLES = ["bronze_transactions", "silver_transactions", "silver_quarantine"]


@dataclass
class TableState:
    data_files: int
    avg_file_mb: float
    snapshots: int
    rows: int


def state(spark: SparkSession, ns: str, name: str) -> TableState:
    t = table(ns, name)
    files = spark.sql(f"SELECT count(*) AS n, coalesce(avg(file_size_in_bytes), 0) AS avg_bytes FROM {t}.files").first()
    return TableState(
        data_files=files["n"],
        avg_file_mb=round(files["avg_bytes"] / 1024 / 1024, 2),
        snapshots=scalar(spark, f"SELECT count(*) FROM {t}.snapshots"),
        rows=scalar(spark, f"SELECT count(*) FROM {t}"),
    )


def maintain(spark: SparkSession, ns: str, name: str, retain_last: int) -> dict:
    ident = f"{ns}.{name}"
    before = state(spark, ns, name)
    rewrite = spark.sql(f"""
        CALL local.system.rewrite_data_files(
            table => '{ident}',
            options => map('min-input-files', '2')
        )
    """).first()
    spark.sql(f"CALL local.system.rewrite_manifests('{ident}')")
    # 위 두 작업도 스냅샷을 만들기 때문에 만료는 마지막에 한다.
    # 프로시저 인자에는 함수를 쓸 수 없어서 현재 시각을 리터럴로 넘긴다.
    now = scalar(spark, "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')")
    expire = spark.sql(f"""
        CALL local.system.expire_snapshots(
            table => '{ident}',
            older_than => TIMESTAMP '{now}',
            retain_last => {retain_last}
        )
    """).first()
    after = state(spark, ns, name)
    if before.rows != after.rows:
        raise RuntimeError(f"{name}: 유지보수 전후 행 수가 다릅니다 ({before.rows} → {after.rows})")
    return {
        "table": name,
        "before": before,
        "after": after,
        "rewritten_files": rewrite["rewritten_data_files_count"],
        "added_files": rewrite["added_data_files_count"],
        "deleted_data_files": expire["deleted_data_files_count"],
    }


def run(spark: SparkSession, ns: str, out_path: str, retain_last: int = 2) -> str:
    results = [maintain(spark, ns, name, retain_last) for name in TABLES]
    lines = [
        "# Iceberg 유지보수 실행 결과",
        "",
        f"`python -m paysim_pipeline.maintenance` 실행 출력입니다. 스냅샷은 최근 {retain_last}개만 남깁니다.",
        "",
        "| 테이블 | 데이터 파일 (전 → 후) | 평균 파일 크기 MB (전 → 후) | 스냅샷 (전 → 후) | 행 수 (전 = 후) | 합친 파일 | 새로 쓴 파일 | 만료로 지운 파일 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        b, a = r["before"], r["after"]
        lines.append(
            f"| {r['table']} | {b.data_files} → {a.data_files} | {b.avg_file_mb} → {a.avg_file_mb} "
            f"| {b.snapshots} → {a.snapshots} | {a.rows:,} | {r['rewritten_files']} | {r['added_files']} "
            f"| {r['deleted_data_files']} |"
        )
    text = "\n".join(lines) + "\n"
    if out_path:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
    return text


def main() -> None:
    p = argparse.ArgumentParser(description="Iceberg 테이블 compaction·스냅샷 만료")
    p.add_argument("--warehouse", default="warehouse")
    p.add_argument("--namespace", default="paysim")
    p.add_argument("--retain-last", type=int, default=2)
    p.add_argument("--out", default="docs/maintenance.md", help="빈 문자열이면 파일로 남기지 않음")
    args = p.parse_args()
    spark = build_spark(args.warehouse)
    try:
        print(run(spark, args.namespace, args.out, args.retain_last))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
