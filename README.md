# PaySim → Apache Iceberg 적재 파이프라인 (PySpark)

모바일 송금 거래 **636만 건**을 PySpark로 읽어 **Apache Iceberg** 테이블에 bronze → silver → gold 순서로 적재한 개인 프로젝트입니다.
하루치 거래가 매일 들어온다고 가정하고, 데이터 엔지니어링 업무에서 중요한 **증분 적재, 재실행 안전성(멱등성), 데이터 품질 검사, 테이블 이력 관리**를 직접 구현하고 전체 데이터로 검증했습니다.

> 데이터: [PaySim (Kaggle, ealaxi/paysim1)](https://www.kaggle.com/datasets/ealaxi/paysim1). 실제 모바일 머니 거래 로그를 바탕으로 **시뮬레이터가 만든 합성 데이터**이며 실제 고객 거래가 아닙니다. 데이터 파일은 레포에 포함하지 않습니다.

## 한눈에 보기

| 항목 | 결과 |
|---|---|
| 적재한 원본 행 | **6,362,620**행 (31일치, 7일 단위 배치 5번) |
| silver 적재 / 격리 / 중복 | 6,362,604 / 16 / 0. **원본 = silver + 격리 + 중복**, 사라진 행 없음 |
| 전체 적재 시간 | 약 **4분 30초** (배치 5개 합계 269.6초, GitHub Codespaces 4코어) |
| 같은 배치 재실행 | silver에 새로 들어간 행 **0건**, 전체 행 수 변화 없음 |
| 품질 검사 | 배치마다 7개 검사, 모든 배치 통과 |
| Iceberg 기능 | 스냅샷 이력, Time travel, Schema/Partition evolution, Rollback 실행 결과 기록 |

실제 실행 출력: [docs/results.md](docs/results.md) · [docs/iceberg_demo.md](docs/iceberg_demo.md) · 배치별 JSON([1~7일](docs/batch_1_7.json), [8~14일](docs/batch_8_14.json), [15~21일](docs/batch_15_21.json), [22~28일](docs/batch_22_28.json), [29~31일](docs/batch_29_31.json), [재실행](docs/rerun_1_7.json))

## 구조

```
PaySim CSV (step = 시뮬레이션 1시간, 1~743)
   │  7일 단위 배치로 나눠 적재 (sim_day = (step-1)//24 + 1)
   ▼
bronze_transactions      원본을 문자열 그대로 보관 + 해시 ID·파일명·배치 ID·적재 시각
   │  형 변환, 규칙 검사, 중복 제거
   ├──────────────► silver_quarantine   검사에 걸린 행을 사유와 함께 보관
   ▼
silver_transactions      거래 1건 = 1행, MERGE INTO로 적재
   │
   ├──► gold_daily_type_summary   일자 × 거래 유형별 건수·금액·사기 건수
   └──► gold_rule_evaluation      단순 탐지 규칙의 정밀도·재현율
                         ▲
dq_results               배치마다 품질 검사 결과 기록 (실패 시 배치 중단)
```

모든 테이블은 로컬 Iceberg Hadoop 카탈로그(`local`)에 있습니다.

## 설계에서 신경 쓴 점

| 문제 | 처리 방법 | 코드 |
|---|---|---|
| PaySim에는 거래 ID가 없음 | 원본 행 11개 칼럼 전체의 SHA-256 해시를 `tx_id`로 사용. 내용이 완전히 같은 행은 한 건으로 봄 | [bronze.py](src/paysim_pipeline/bronze.py) |
| 같은 배치를 다시 돌리면 중복이 쌓임 | bronze는 `sim_day` 파티션 단위 덮어쓰기, silver는 `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` | [bronze.py](src/paysim_pipeline/bronze.py), [silver.py](src/paysim_pipeline/silver.py) |
| 잘못된 값을 조용히 버리면 원인을 못 찾음 | 형 변환 실패·음수/0 금액·알 수 없는 유형·잔액 누락 등은 `silver_quarantine`에 사유와 함께 보관 | [silver.py](src/paysim_pipeline/silver.py) |
| 금액 계산 오차 | 금액·잔액은 `double`이 아닌 `decimal(18,2)` | [schema.py](src/paysim_pipeline/schema.py) |
| 적재 중 행이 사라지지 않았는지 | `bronze 행 = silver 행 + 격리 행 + 중복 행`을 배치마다 검사, gold 합계와 silver 행 수도 대조 | [quality.py](src/paysim_pipeline/quality.py) |
| 원본 잔액 기록이 어긋난 거래 | `orig_balance_gap`(거래 전 잔액 − 금액 − 거래 후 잔액) 칼럼으로 표시 | [silver.py](src/paysim_pipeline/silver.py) |

## 결과

### 배치별 적재

| 배치 | silver 적재 | 격리 | 중복 | 실행 시간 |
|---|---:|---:|---:|---:|
| 1~7일차 | 1,930,180 | 0 | 0 | 68.8초 |
| 8~14일차 | 2,854,592 | 3 | 0 | 87.0초 |
| 15~21일차 | 1,279,258 | 3 | 0 | 56.1초 |
| 22~28일차 | 232,129 | 6 | 0 | 31.7초 |
| 29~31일차 | 66,445 | 4 | 0 | 26.0초 |
| **합계** | **6,362,604** | **16** | **0** | **269.6초** |

- **격리 16건**: 모두 금액이 0인 `CASH_OUT` 거래(`non_positive_amount`)입니다. 버리지 않고 원본 값과 사유를 함께 남겼습니다.
- **재실행(1~7일차, 60.9초)**: silver에 스냅샷은 하나 더 생겼지만 추가된 행은 없고 전체 행 수는 6,362,604로 그대로입니다. `tx_id` 해시와 `MERGE INTO` 덕분에 같은 배치를 다시 돌려도 중복이 쌓이지 않습니다.

### 데이터에서 발견한 것: 잔액 기록 불일치

`orig_balance_gap`이 0이 아닌 거래, 즉 거래 전후 잔액이 금액만큼 움직이지 않은 거래가 **전체의 약 62%(3,939,445건)**였습니다.

| 거래 유형 | 거래 수 | 잔액 불일치 | 비율 |
|---|---:|---:|---:|
| CASH_IN | 1,399,284 | 213,177 | 15.2% |
| CASH_OUT | 2,237,484 | 1,998,093 | 89.3% |
| DEBIT | 41,432 | 12,895 | 31.1% |
| PAYMENT | 2,151,495 | 1,205,652 | 56.0% |
| TRANSFER | 532,909 | 509,628 | 95.6% |

이 값으로 거래를 격리하면 데이터 대부분이 빠지기 때문에 **격리 대상이 아니라 칼럼으로 표시**하도록 정했습니다. 잔액을 근거로 하는 분석은 이 칼럼으로 걸러서 쓰도록 했습니다.

### 단순 탐지 규칙 비교 (`gold_rule_evaluation`)

전체 사기 거래 8,197건 기준입니다.

| 규칙 | 잡은 사기(TP) | 오탐(FP) | 놓친 사기(FN) | 정밀도 | 재현율 |
|---|---:|---:|---:|---:|---:|
| `paysim_flag` (원본에 있던 플래그) | 16 | 0 | 8,181 | 1.00 | 0.2% |
| `full_balance_drain` (TRANSFER/CASH_OUT으로 잔액을 한 번에 0으로 비움) | 8,008 | 0 | 189 | 1.00 | 97.7% |

원본 플래그는 사기의 0.2%만 잡았고, 잔액 전액 인출 규칙은 97.7%를 오탐 없이 잡았습니다. 다만 이 결과는 **시뮬레이터가 사기를 "계좌를 비우는 거래"로 만들었기 때문에 생긴 특성**일 가능성이 큽니다. 실제 거래 데이터에서는 이렇게 깔끔하게 나오지 않을 것이라 보고, 규칙 성능보다는 **gold 계층에서 규칙을 같은 기준으로 비교하는 구조**를 만든 데 의미를 두었습니다.

## Iceberg 기능

[iceberg_demo.py](src/paysim_pipeline/iceberg_demo.py)가 적재된 테이블로 아래를 실행하고 실제 출력을 [docs/iceberg_demo.md](docs/iceberg_demo.md)에 남깁니다.
원본 테이블을 건드리지 않도록 schema·partition 변경과 rollback은 복제 테이블(`demo_transactions`, 1~3일차)에서 합니다.

| 기능 | 한 일 | 결과 |
|---|---|---|
| 스냅샷 이력 | 배치마다 silver에 들어간 행 수 조회 | 스냅샷 6개 (배치 5개 + 재실행 1개, 재실행은 추가 행 0) |
| Time travel | `VERSION AS OF`로 첫 배치 시점 조회 | 첫 스냅샷 1,930,180행 / 현재 6,362,604행 |
| Schema evolution | `review_status` 칼럼 추가 후 사기 거래에 `UPDATE` | 890행에 값 입력, 기존 데이터 파일은 다시 쓰지 않음 |
| Partition evolution | `sim_day` 파티션에 `tx_type` 추가 후 4일차 적재 | 기존 파일(spec 0) 3개·1,030,563행과 새 파일(spec 1) 5개·28,240행이 한 테이블에 공존 |
| Rollback | `rollback_to_snapshot`으로 UPDATE 이전으로 복구 | 복구 후 `review_status` 값이 있는 행 0 |

## 문제 해결 기록

- **배치 결과 JSON이 깨지던 문제**: 처음에는 `run_all.sh`에서 실행 결과를 `tee`로 받아 JSON 파일로 저장했습니다. 그런데 Spark가 의존성(Iceberg 런타임)을 불러오며 남기는 `:: loading settings ...` 로그가 표준 출력에 섞여 파일이 JSON으로 읽히지 않았습니다. 결과를 `--output` 옵션으로 파일에 따로 쓰도록 바꿨습니다. ([run.py](src/paysim_pipeline/run.py))
- **결과 숫자를 손으로 옮기지 않도록**: 행 대조, 격리 사유, 잔액 불일치, 규칙 비교를 테이블에서 바로 뽑는 [report.py](src/paysim_pipeline/report.py)를 만들어 README 숫자의 근거를 [docs/results.md](docs/results.md)로 남겼습니다.

## 한계와 다음에 해 볼 것

- 로컬 Hadoop 카탈로그라 동시에 여러 작업이 쓰는 상황은 다루지 않았습니다. REST/Glue 카탈로그와 S3로 옮겨 보고 싶습니다.
- 배치마다 CSV 전체를 읽고 날짜로 거릅니다. 실제 환경처럼 일자별 파일로 나눠 들어오는 구조로 바꾸면 읽는 양을 줄일 수 있습니다.
- 작은 파일이 쌓이면 Iceberg `rewrite_data_files`·`expire_snapshots` 같은 유지보수 작업이 필요합니다. 아직 적용하지 않았습니다.
- 배치 실행은 셸 스크립트로 순서대로 돌립니다. Airflow 같은 스케줄러로 옮기면 재시도와 백필을 더 체계적으로 할 수 있습니다.

## 실행 방법 (GitHub Codespaces)

Spark에 Java가 필요해서 Java 17과 Python 3.11이 들어 있는 devcontainer를 준비해 두었습니다.

1. 이 레포에서 **Code → Codespaces → Create codespace** (4코어 / 16GB 권장)
2. Kaggle API 키를 **Settings → Codespaces → Secrets**에 `KAGGLE_USERNAME`, `KAGGLE_KEY`로 등록
3. 터미널에서:

```bash
bash scripts/download_paysim.sh   # data/raw/ 로 내려받기
bash scripts/run_all.sh           # 7일 단위 배치 5번 + 1~7일차 재실행 + 결과 리포트 + Iceberg 기능 실행
```

배치 하나만 돌릴 때:

```bash
python -m paysim_pipeline.run --source data/raw/<파일명>.csv --from-day 1 --to-day 7 --output docs/batch_1_7.json
```

테스트 (손으로 만든 15행 샘플, Kaggle 데이터 필요 없음):

```bash
pytest -v
```

GitHub Actions가 push마다 같은 테스트를 실행합니다.

## 기술 스택

PySpark 3.5 · Apache Iceberg 1.6 (Hadoop catalog) · pytest · GitHub Actions · Dev Containers
