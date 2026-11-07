from paysim_pipeline import report
from paysim_pipeline.run import run_batch


def test_report_runs(spark, sample_csv, ns, tmp_path):
    run_batch(spark, sample_csv, ns, 1, 3, max_quarantine_ratio=0.5)

    text = report.run(spark, ns, str(tmp_path / "results.md"))

    assert "| silver_transactions | 10 |" in text
    assert "| non_positive_amount | PAYMENT | 1 |" in text
    assert "full_balance_drain" in text and "paysim_flag" in text
