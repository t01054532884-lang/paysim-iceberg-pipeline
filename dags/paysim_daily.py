"""PaySim 일 단위 적재 DAG.

실행 날짜(logical date) 하루 = 시뮬레이션 하루로 보고, 2026-01-01을 sim_day 1로 맞춘다.
catchup=True라서 31일치를 백필할 수 있고, 적재가 멱등이라 같은 날짜를 다시 돌려도 결과가 같다.

load_day ──► is_week_end ──► maintain
              (7일마다·마지막 날만 통과)

- max_active_runs=1: 로컬 Hadoop 카탈로그는 동시에 여러 작업이 커밋하는 상황을 다루지 않으므로 한 번에 한 날짜만 돌린다.
- 각 작업은 별도 프로세스로 Spark를 띄운다. Airflow는 가상환경을 따로 두고, 파이프라인은 PAYSIM_PYTHON으로 실행한다.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

PROJECT_DIR = os.environ.get("PAYSIM_PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PYTHON = os.environ.get("PAYSIM_PYTHON", "python")
SOURCE = os.environ.get("PAYSIM_SOURCE", "data/raw/PS_20174392719_1491204439457_log.csv")

SIM_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
LAST_SIM_DAY = 31


def sim_day(ds: str) -> int:
    """'2026-01-07' → 7"""
    return (datetime.fromisoformat(ds).date() - SIM_START.date()).days + 1


def is_week_end(ds: str) -> bool:
    day = sim_day(ds)
    return day % 7 == 0 or day == LAST_SIM_DAY


with DAG(
    dag_id="paysim_daily",
    description="PaySim 하루치 거래를 Iceberg에 적재하고 7일마다 테이블 유지보수",
    start_date=SIM_START,
    end_date=SIM_START + timedelta(days=LAST_SIM_DAY - 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    user_defined_macros={"sim_day": sim_day},
    tags=["paysim", "iceberg"],
) as dag:
    load_day = BashOperator(
        task_id="load_day",
        cwd=PROJECT_DIR,
        bash_command=(
            f"{PYTHON} -m paysim_pipeline.run --source {SOURCE}"
            " --from-day {{ sim_day(ds) }} --to-day {{ sim_day(ds) }}"
            " --output logs/runs/{{ ds }}.json"
        ),
    )

    week_end = ShortCircuitOperator(task_id="is_week_end", python_callable=is_week_end)

    maintain = BashOperator(
        task_id="maintain",
        cwd=PROJECT_DIR,
        bash_command=f"{PYTHON} -m paysim_pipeline.maintenance --out logs/maintenance/{{{{ ds }}}}.md",
    )

    load_day >> week_end >> maintain
