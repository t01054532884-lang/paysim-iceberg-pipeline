from paysim_pipeline import iceberg_demo
from paysim_pipeline.run import run_batch


def test_iceberg_demo_runs(spark, sample_csv, ns, tmp_path):
    run_batch(spark, sample_csv, ns, 1, 1, max_quarantine_ratio=0.5)
    run_batch(spark, sample_csv, ns, 2, 3, max_quarantine_ratio=0.5)

    text = iceberg_demo.run(spark, ns, str(tmp_path / "demo.md"), demo_days=2)

    assert "첫 스냅샷" in text
    assert "값이 들어간 행 수: 3" in text  # 1~2일차 사기 거래 3건
    assert "review_status 값이 있는 행 수: 0" in text  # rollback 후
