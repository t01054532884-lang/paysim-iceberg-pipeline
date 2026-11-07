#!/usr/bin/env bash
# 31일치 PaySim을 7일 단위 배치 5번으로 나눠 적재한 뒤, 결과 숫자와 Iceberg 기능 실행 결과를 docs/에 남긴다.
set -euo pipefail

SOURCE="${1:-$(ls data/raw/*.csv | head -1)}"
echo "source: $SOURCE"
mkdir -p docs

for from in 1 8 15 22 29; do
  to=$(( from + 6 ))
  if [ "$to" -gt 31 ]; then to=31; fi
  python -m paysim_pipeline.run --source "$SOURCE" --from-day "$from" --to-day "$to" --output "docs/batch_${from}_${to}.json"
done

# 같은 배치를 한 번 더 돌려도 결과가 같은지 확인
python -m paysim_pipeline.run --source "$SOURCE" --from-day 1 --to-day 7 --output docs/rerun_1_7.json

python -m paysim_pipeline.report
python -m paysim_pipeline.iceberg_demo

# 작은 파일 합치기·스냅샷 만료 (Iceberg 기능 실행 뒤에 해야 time travel 결과가 남는다)
python -m paysim_pipeline.maintenance
