from __future__ import annotations

import itertools
import os
from pathlib import Path

import pytest

from meterlake.config import Settings

_ns = itertools.count()


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="session")
def spark(warehouse: Path):
    pytest.importorskip("pyspark")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    from meterlake.spark import get_spark

    session = get_spark(warehouse, "meterlake-tests")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path: Path, warehouse: Path) -> Settings:
    """A fresh namespace per test, so tests never see each other's tables."""
    return Settings(landing=tmp_path / "landing", warehouse=warehouse, namespace=f"t{next(_ns)}")
