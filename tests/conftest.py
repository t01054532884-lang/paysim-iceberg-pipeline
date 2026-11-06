import csv
import uuid

import pytest

from paysim_pipeline.schema import RAW_COLUMNS
from paysim_pipeline.spark import build_spark

# PaySim과 같은 형식의 작은 테스트용 데이터 (값은 직접 만든 것)
# 1일차 = step 1~24, 2일차 = 25~48, 3일차 = 49~72
SAMPLE_ROWS = [
    # 1일차: 정상 5건 + 완전 중복 1건 + 금액 0 1건
    ("1", "PAYMENT", "1000.00", "C100", "5000.00", "4000.00", "M200", "0.00", "0.00", "0", "0"),
    ("2", "TRANSFER", "3000.00", "C101", "3000.00", "0.00", "C201", "0.00", "0.00", "1", "0"),
    ("2", "CASH_OUT", "3000.00", "C102", "3000.00", "0.00", "C202", "100.00", "3100.00", "1", "0"),
    ("5", "CASH_IN", "500.00", "C103", "100.00", "600.00", "C203", "800.00", "300.00", "0", "0"),
    ("10", "DEBIT", "200.00", "C104", "1000.00", "800.00", "C204", "0.00", "200.00", "0", "0"),
    ("10", "DEBIT", "200.00", "C104", "1000.00", "800.00", "C204", "0.00", "200.00", "0", "0"),
    ("12", "PAYMENT", "0.00", "C112", "100.00", "100.00", "M212", "0.00", "0.00", "0", "0"),
    # 2일차: 정상 3건 + 알 수 없는 유형 1건
    ("25", "TRANSFER", "250000.00", "C105", "250000.00", "0.00", "C205", "0.00", "0.00", "1", "1"),
    ("30", "PAYMENT", "150.50", "C106", "1000.00", "849.50", "M206", "0.00", "0.00", "0", "0"),
    ("30", "WIRE", "10.00", "C113", "100.00", "90.00", "C213", "0.00", "10.00", "0", "0"),
    ("40", "CASH_OUT", "700.00", "C107", "1000.00", "300.00", "C207", "0.00", "700.00", "0", "0"),
    # 3일차: 정상 2건 + 잔액 누락 1건
    ("49", "TRANSFER", "900.00", "C108", "5000.00", "4100.00", "C208", "0.00", "900.00", "0", "0"),
    ("60", "CASH_OUT", "1200.00", "C109", "1200.00", "0.00", "C209", "0.00", "1200.00", "0", "0"),
    ("72", "PAYMENT", "80.00", "C110", "", "", "M210", "0.00", "0.00", "0", "0"),
    # step을 읽을 수 없는 행 → sim_day 0
    ("abc", "PAYMENT", "10.00", "C111", "100.00", "90.00", "M211", "0.00", "0.00", "0", "0"),
]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = build_spark(str(tmp_path_factory.mktemp("warehouse")), app_name="paysim-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_csv(tmp_path_factory):
    path = tmp_path_factory.mktemp("raw") / "paysim_sample.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(RAW_COLUMNS)
        w.writerows(SAMPLE_ROWS)
    return str(path)


@pytest.fixture
def ns():
    """테스트마다 별도 네임스페이스를 써서 서로 영향을 주지 않게 한다."""
    return f"t_{uuid.uuid4().hex[:8]}"
