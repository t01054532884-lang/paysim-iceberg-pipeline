# Airflow DAG 실행 결과

`bash scripts/airflow_dags_test.sh <날짜>` (`airflow dags test`) 출력에서 작업 상태 줄만 옮겼습니다.

## 2026-01-07 (sim_day 7)

```
DagRun Finished: dag_id=paysim_daily, execution_date=2026-01-07 00:00:00+00:00
state=success
```

load_day 결과:

```json
{
  "batch_id": "days_007_007",
  "silver": {
    "valid_rows": 420583,
    "duplicate_rows": 0,
    "quarantined_rows": 0
  },
  "checks": [
    {
      "check_name": "row_reconciliation_diff",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_duplicate_tx_id",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_null_required",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_non_positive_amount",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_unknown_tx_type",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "gold_vs_silver_count_diff",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "quarantine_ratio",
      "metric": 0.0,
      "threshold": 0.01,
      "passed": true
    }
  ],
  "seconds": 68.2
}
```

## 2026-01-08 (sim_day 8)

```
DagRun Finished: dag_id=paysim_daily, execution_date=2026-01-08 00:00:00+00:00
state=success
```

load_day 결과:

```json
{
  "batch_id": "days_008_008",
  "silver": {
    "valid_rows": 449637,
    "duplicate_rows": 0,
    "quarantined_rows": 0
  },
  "checks": [
    {
      "check_name": "row_reconciliation_diff",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_duplicate_tx_id",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_null_required",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_non_positive_amount",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "silver_unknown_tx_type",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "gold_vs_silver_count_diff",
      "metric": 0.0,
      "threshold": 0.0,
      "passed": true
    },
    {
      "check_name": "quarantine_ratio",
      "metric": 0.0,
      "threshold": 0.01,
      "passed": true
    }
  ],
  "seconds": 35.8
}
```
