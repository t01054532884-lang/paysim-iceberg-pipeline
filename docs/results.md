# 적재 결과

`python -m paysim_pipeline.report` 실행 출력입니다.

## 1. 행 수

| 테이블 | 행 수 |
|---|---|
| bronze_transactions (원본) | 6,362,620 |
| silver_transactions | 6,362,604 |
| silver_quarantine | 16 |
| 중복 (bronze − silver − 격리) | 0 |

## 2. 격리 사유

| reason | raw_type | rows |
|---|---|---|
| non_positive_amount | CASH_OUT | 16 |

## 3. 원본 잔액 기록이 어긋난 거래

`orig_balance_gap` = 거래 전 잔액 − 금액 − 거래 후 잔액 (CASH_IN은 + 금액). 0이 아니면 잔액 기록이 금액만큼 움직이지 않은 거래입니다.

| tx_type | tx_count | gap_count |
|---|---|---|
| CASH_IN | 1399284 | 213177 |
| CASH_OUT | 2237484 | 1998093 |
| DEBIT | 41432 | 12895 |
| PAYMENT | 2151495 | 1205652 |
| TRANSFER | 532909 | 509628 |

## 4. 탐지 규칙 비교 (전체 silver 기준)

| rule_name | tp | fp | fn | precision | recall |
|---|---|---|---|---|---|
| full_balance_drain | 8008 | 0 | 189 | 1.0 | 0.9769 |
| paysim_flag | 16 | 0 | 8181 | 1.0 | 0.002 |
