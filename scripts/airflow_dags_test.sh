#!/usr/bin/env bash
# Airflow를 별도 가상환경에 설치하고, 스케줄러 없이 DAG 한 번(하루치)을 실행해 본다.
#   bash scripts/airflow_dags_test.sh 2026-01-07   # sim_day 7 → 적재 + 유지보수까지 실행
set -euo pipefail

AIRFLOW_VERSION=2.10.5
PY_VERSION=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
VENV=.venv-airflow

if [ ! -x "$VENV/bin/airflow" ]; then
  python -m venv "$VENV"
  "$VENV/bin/pip" install -q "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"
fi

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PAYSIM_PROJECT_DIR="$PWD"
export PAYSIM_PYTHON="$(command -v python)"   # 파이프라인은 PySpark가 설치된 기본 Python으로 실행
export PAYSIM_SOURCE="$(ls "$PWD"/data/raw/*.csv | head -1)"

"$VENV/bin/airflow" db migrate > /dev/null
"$VENV/bin/airflow" dags list-import-errors
"$VENV/bin/airflow" dags test paysim_daily "${1:-2026-01-07}"
