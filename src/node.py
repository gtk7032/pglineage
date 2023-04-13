from __future__ import annotations

from typing import Any, Dict, List

import table
from restarget import ResTarget


class Select:
    def __init__(
        self,
        layer: int,
        columns: List[ResTarget],
        tables: List["table.Table"],
    ) -> None:
        self.layer = layer
        self.columns = columns
        self.tables = tables

    @staticmethod
    def empty() -> Select:
        return Select(-1, [], [])

    def format(self) -> Dict[str, Any]:
        return {
            "layer": self.layer,
            "columns": [c.format() for c in self.columns],
            "tables": [t.format() for t in self.tables],
        }


class Insert:
    def __init__(
        self,
        layer: int,
        tgtcols: List[ResTarget],
        tgttbl: "table.Table",
        select: Select,
    ) -> None:
        self.layer = layer
        self.tgtcols = tgtcols
        self.tgttbl = tgttbl
        self.select = select

    def format(self) -> Dict[str, Any]:
        return {
            "layer": self.layer,
            "tgtcols": [col.format() for col in self.tgtcols],
            "tgttbl": self.tgttbl.format(),
            "select": self.select.format(),
        }
