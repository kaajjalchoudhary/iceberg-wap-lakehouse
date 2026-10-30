"""Where things live. Table names are derived from one catalog + namespace."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

CATALOG = "lake"


@dataclass(frozen=True)
class Settings:
    landing: Path
    warehouse: Path
    namespace: str = "energy"

    @classmethod
    def from_env(cls) -> Settings:
        root = Path(os.getenv("LAKE_ROOT", "lake")).resolve()
        return cls(landing=root / "landing", warehouse=root / "warehouse", namespace=os.getenv("LAKE_NAMESPACE", "energy"))

    def table(self, name: str) -> str:
        """Fully qualified name, e.g. lake.energy.readings."""
        return f"{CATALOG}.{self.namespace}.{name}"

    def ident(self, name: str) -> str:
        """Name inside the catalog, as stored procedures expect it, e.g. energy.readings."""
        return f"{self.namespace}.{name}"
