"""PaySim 원본 칼럼과 테이블 이름 정의."""
from pyspark.sql.types import DecimalType

# Kaggle PaySim CSV 헤더 순서 그대로
RAW_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

TX_TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
HOURS_PER_DAY = 24  # PaySim의 step 1 = 시뮬레이션 1시간

MONEY = DecimalType(18, 2)
MONEY_SUM = DecimalType(20, 2)

SILVER_COLUMNS = [
    "tx_id",
    "sim_day",
    "step",
    "hour_of_day",
    "tx_type",
    "amount",
    "orig_account",
    "orig_balance_before",
    "orig_balance_after",
    "dest_account",
    "dest_balance_before",
    "dest_balance_after",
    "dest_is_merchant",
    "orig_balance_gap",
    "is_fraud",
    "is_flagged_fraud",
    "_batch_id",
    "_loaded_at",
]

QUARANTINE_COLUMNS = RAW_COLUMNS + ["tx_id", "sim_day", "reason", "_batch_id", "_quarantined_at"]


def table(ns: str, name: str) -> str:
    return f"local.{ns}.{name}"
