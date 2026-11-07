from paysim_pipeline import maintenance
from paysim_pipeline.run import run_batch


def test_maintenance_compacts_and_expires(spark, sample_csv, ns, tmp_path):
    # 하루씩 3번 + 재실행 1번 → silver 스냅샷 4개, 파티션마다 파일이 따로 생김
    for day in (1, 2, 3, 1):
        run_batch(spark, sample_csv, ns, day, day, max_quarantine_ratio=0.5)
    before = maintenance.state(spark, ns, "silver_transactions")

    text = maintenance.run(spark, ns, str(tmp_path / "m.md"), retain_last=1)

    after = maintenance.state(spark, ns, "silver_transactions")
    assert after.rows == before.rows == 10
    assert after.snapshots == 1 < before.snapshots
    assert "silver_transactions" in text
