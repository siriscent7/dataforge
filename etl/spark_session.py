"""Creates a configured local SparkSession."""
import os
from pyspark.sql import SparkSession

os.environ.setdefault("SPARK_SUBMIT_OPTS", "-Djava.security.manager=allow")


def get_spark(app_name: str = "DataForge") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
