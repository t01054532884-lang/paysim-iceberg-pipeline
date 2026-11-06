"""Iceberg 테이블 DDL. 여러 번 실행해도 안전하도록 IF NOT EXISTS만 쓴다."""
from pyspark.sql import SparkSession

from .schema import table

RAW_STRING_COLUMNS = """
    step string, type string, amount string,
    nameOrig string, oldbalanceOrg string, newbalanceOrig string,
    nameDest string, oldbalanceDest string, newbalanceDest string,
    isFraud string, isFlaggedFraud string
"""


def create_all(spark: SparkSession, ns: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")

    # bronze: 원본을 문자열 그대로 보관 + 적재 이력 칼럼
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table(ns, 'bronze_transactions')} (
            {RAW_STRING_COLUMNS},
            sim_day int,
            _row_hash string,
            _source_file string,
            _batch_id string,
            _ingested_at timestamp
        ) USING iceberg
        PARTITIONED BY (sim_day)
    """)

    # silver: 형 변환·정제가 끝난 거래 1건 = 1행 (tx_id 유일)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table(ns, 'silver_transactions')} (
            tx_id string,
            sim_day int,
            step int,
            hour_of_day int,
            tx_type string,
            amount decimal(18,2),
            orig_account string,
            orig_balance_before decimal(18,2),
            orig_balance_after decimal(18,2),
            dest_account string,
            dest_balance_before decimal(18,2),
            dest_balance_after decimal(18,2),
            dest_is_merchant boolean,
            orig_balance_gap decimal(18,2),
            is_fraud boolean,
            is_flagged_fraud boolean,
            _batch_id string,
            _loaded_at timestamp
        ) USING iceberg
        PARTITIONED BY (sim_day)
    """)

    # silver로 못 간 행을 사유와 함께 보관
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table(ns, 'silver_quarantine')} (
            {RAW_STRING_COLUMNS},
            tx_id string,
            sim_day int,
            reason string,
            _batch_id string,
            _quarantined_at timestamp
        ) USING iceberg
        PARTITIONED BY (sim_day)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table(ns, 'gold_daily_type_summary')} (
            sim_day int,
            tx_type string,
            tx_count bigint,
            total_amount decimal(20,2),
            avg_amount decimal(18,2),
            fraud_count bigint,
            fraud_amount decimal(20,2),
            flagged_count bigint
        ) USING iceberg
        PARTITIONED BY (sim_day)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table(ns, 'gold_rule_evaluation')} (
            rule_name string,
            tp bigint, fp bigint, fn bigint, tn bigint,
            precision double,
            recall double,
            evaluated_at timestamp
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table(ns, 'dq_results')} (
            batch_id string,
            check_name string,
            metric double,
            threshold double,
            passed boolean,
            checked_at timestamp
        ) USING iceberg
    """)
