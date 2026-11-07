# Iceberg 기능 실행 결과

`python -m paysim_pipeline.iceberg_demo` 실행 출력입니다.

## 1. silver_transactions 스냅샷 이력

| committed_at | snapshot_id | operation | added_records | total_records |
|---|---|---|---|---|
| 2026-11-07 15:55:33.185000 | 2081263603667034418 | append | 1930180 | 1930180 |
| 2026-11-07 15:57:04.001000 | 2791832841491606868 | append | 2854592 | 4784772 |
| 2026-11-07 15:58:15.290000 | 107003344372153175 | append | 1279258 | 6064030 |
| 2026-11-07 15:58:56.832000 | 218873020737285212 | append | 232129 | 6296159 |
| 2026-11-07 15:59:30.105000 | 7126925651240622699 | append | 66445 | 6362604 |
| 2026-11-07 16:00:26.777000 | 6875013491027201021 | append |  | 6362604 |

## 2. Time travel

- 첫 스냅샷(2081263603667034418) 기준 행 수: 1,930,180
- 현재 행 수: 6,362,604

## 3. Schema evolution

- `demo_transactions`(1~3일차 복제)에 `review_status` 칼럼을 추가하고 사기 거래에 값을 넣음
- 값이 들어간 행 수: 890
- 기존 데이터 파일은 다시 쓰지 않고 칼럼이 추가됨 (ALTER TABLE은 메타데이터만 변경)

## 4. Partition evolution

`tx_type` 파티션을 추가한 뒤 4일차를 넣음. 기존 파일은 옛 파티션 규칙(spec 0), 새 파일은 새 규칙(spec 1)으로 함께 존재합니다.

| spec_id | data_files | records |
|---|---|---|
| 0 | 3 | 1030563 |
| 1 | 5 | 28240 |

## 5. Rollback

- UPDATE 이전 스냅샷(343407966227121390)으로 되돌린 뒤 review_status 값이 있는 행 수: 0

## 마지막 배치 품질 검사

| batch_id | check_name | metric | threshold | passed |
|---|---|---|---|---|
| days_001_007 | gold_vs_silver_count_diff | 0.0 | 0.0 | True |
| days_001_007 | quarantine_ratio | 0.0 | 0.01 | True |
| days_001_007 | row_reconciliation_diff | 0.0 | 0.0 | True |
| days_001_007 | silver_duplicate_tx_id | 0.0 | 0.0 | True |
| days_001_007 | silver_non_positive_amount | 0.0 | 0.0 | True |
| days_001_007 | silver_null_required | 0.0 | 0.0 | True |
| days_001_007 | silver_unknown_tx_type | 0.0 | 0.0 | True |
