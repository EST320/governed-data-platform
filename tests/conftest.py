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
