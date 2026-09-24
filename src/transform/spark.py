"""One place to build the local Spark session, so every job and test uses the
same settings."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "governed-data-platform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        # Local data is small per partition; the default of 200 shuffle partitions
        # would create hundreds of tiny tasks and files.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        # Store and compare timestamps in UTC regardless of the machine's timezone.
        .config("spark.sql.session.timeZone", "UTC")
        # Overwrite only the partitions a job writes, never the whole table.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
