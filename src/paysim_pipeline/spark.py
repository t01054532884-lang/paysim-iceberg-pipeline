"""Iceberg 카탈로그가 연결된 SparkSession 생성."""
import os

from pyspark.sql import SparkSession

SPARK_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
CATALOG = "local"


def build_spark(warehouse: str, app_name: str = "paysim-iceberg") -> SparkSession:
    """로컬 파일시스템을 웨어하우스로 쓰는 Iceberg Hadoop 카탈로그 `local`을 붙인 세션."""
    warehouse = os.path.abspath(warehouse)
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.jars.packages", SPARK_ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        # 형 변환 실패를 예외 대신 null로 받아 격리(quarantine) 테이블로 보낸다.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
