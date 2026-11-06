# PaySim → Apache Iceberg 적재 파이프라인 (PySpark)

모바일 송금 거래 데이터(PaySim)를 PySpark로 읽어 **Apache Iceberg** 테이블에 bronze → silver → gold 순서로 적재하는 개인 학습 프로젝트입니다.
하루치 거래가 매일 들어온다고 가정하고 **증분 적재, 재실행 안전성(멱등성), 데이터 품질 검사, Iceberg 스냅샷 기능**을 직접 구현해 보는 것이 목표입니다.

> 데이터: [PaySim (Kaggle, ealaxi/paysim1)](https://www.kaggle.com/datasets/ealaxi/paysim1) — 실제 모바일 머니 거래 로그를 바탕으로 **시뮬레이터가 만든 합성 데이터**입니다. 실제 고객 거래가 아닙니다. 데이터 파일은 레포에 포함하지 않습니다.

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

## 실행 방법 (GitHub Codespaces)

Spark에 Java가 필요해서 Java 17과 Python 3.11이 들어 있는 devcontainer를 준비해 두었습니다.

1. 이 레포에서 **Code → Codespaces → Create codespace** (4코어 / 16GB 권장)
2. Kaggle API 키를 **Settings → Codespaces → Secrets**에 `KAGGLE_USERNAME`, `KAGGLE_KEY`로 등록
3. 터미널에서:

```bash
bash scripts/download_paysim.sh   # data/raw/ 로 내려받기
bash scripts/run_all.sh           # 7일 단위 배치 5번 + 1~7일차 재실행 + Iceberg 기능 실행
```

배치 하나만 돌릴 때:

```bash
python -m paysim_pipeline.run --source data/raw/<파일명>.csv --from-day 1 --to-day 7
```

테스트 (손으로 만든 15행 샘플, Kaggle 데이터 필요 없음):

```bash
pytest -v
```

GitHub Actions가 push마다 같은 테스트를 실행합니다.

## Iceberg 기능

[iceberg_demo.py](src/paysim_pipeline/iceberg_demo.py)가 적재된 테이블로 아래를 실행하고 실제 출력을 [docs/iceberg_demo.md](docs/iceberg_demo.md)에 남깁니다.

- **스냅샷 이력**: 배치마다 silver에 들어간 행 수
- **Time travel**: `VERSION AS OF`로 첫 배치 시점의 테이블 조회
- **Schema evolution**: 칼럼 추가 후 `UPDATE`
- **Partition evolution**: `sim_day` 파티션에 `tx_type`을 추가해도 기존 파일은 그대로 두고 새 파일만 새 규칙으로 저장
- **Rollback**: `rollback_to_snapshot`으로 UPDATE 이전 상태로 복구

원본 테이블을 건드리지 않도록 schema·partition 변경과 rollback은 복제 테이블(`demo_transactions`)에서 합니다.

## 결과

> 전체 데이터로 실행한 뒤 채웁니다. 아래 숫자는 실행 결과에서 그대로 옮깁니다.

- 원본 행 수 / silver 행 수 / 격리 행 수(사유별) / 중복 행 수:
- 배치당 실행 시간 (Codespaces 4코어):
- 재실행 시 silver 신규 적재 행 수:
- 탐지 규칙 비교 (`gold_rule_evaluation`):

## 기술 스택

PySpark 3.5 · Apache Iceberg 1.6 (Hadoop catalog) · pytest · GitHub Actions · Dev Containers
