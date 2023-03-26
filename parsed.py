from __future__ import annotations

from typing import Any, Dict, List

from restarget import ResTarget
from table import Table


class ParsedStatement:
    def __init__(
        self,
        layer: int,
        columns: List[ResTarget],
        tables: List[Table],
    ) -> None:
        self.layer = layer
        self.columns = columns
        self.tables = tables

    def format(self) -> Dict[str, Any]:
        return {
            "layer": self.layer,
            "columns": [c.format() for c in self.columns],
            "tables": [t.format() for t in self.tables],
        }
