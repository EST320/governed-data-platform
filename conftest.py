import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    """One local Spark session for the whole test run; starting Spark is slow."""
    from src.transform.spark import get_spark
    import os

    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    session = get_spark("tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def warehouse_dir(spark, tmp_path_factory):
    """Stage the fixture dataset and publish a warehouse, once per test run.
    Tests that modify anything work on a copy."""
    from src.transform.amendments import resolve_amendments
    from src.transform.edgar_staging import stage_quarter
    from src.warehouse.build import publish
    from tests.pipeline_fixtures import QUARTERS, land_fixture

    data_dir = tmp_path_factory.mktemp("warehouse")
    land_fixture(data_dir)
    for year, quarter in QUARTERS:
        stage_quarter(spark, data_dir, year, quarter)
    resolve_amendments(spark, data_dir)
    publish(data_dir, lambda _: None)
    return data_dir
