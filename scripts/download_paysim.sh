#!/usr/bin/env bash
# Kaggle에서 PaySim 데이터를 data/raw/ 로 내려받는다.
# Codespaces Secrets에 KAGGLE_USERNAME, KAGGLE_KEY를 등록해 두어야 한다.
set -euo pipefail

: "${KAGGLE_USERNAME:?KAGGLE_USERNAME 환경변수가 없습니다 (Codespaces Secrets에 등록)}"
: "${KAGGLE_KEY:?KAGGLE_KEY 환경변수가 없습니다 (Codespaces Secrets에 등록)}"

mkdir -p data/raw
kaggle datasets download -d ealaxi/paysim1 -p data/raw --unzip
ls -lh data/raw
