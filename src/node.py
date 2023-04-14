from __future__ import annotations

from typing import Any, TypeAlias

import table
from restarget import ResTarget


class Select:
    def __init__(
        self,
        columns: list[ResTarget],
        tables: list["table.Table"],
        layer: int = 0,
    ) -> None:
        self.columns = columns
        self.tables = tables
        self.layer = layer

    @staticmethod
    def empty() -> Select:
        return Select([], [], -1)

    def format(self) -> dict[str, Any]:
        return {
            "statement": "Select",
            "layer": self.layer,
            "columns": [c.format() for c in self.columns],
            "tables": [t.format() for t in self.tables],
        }


class Insert:
    def __init__(
        self,
        tgtcols: list[ResTarget],
        tgttbl: "table.Table",
        subquery: Select,
    ) -> None:
        self.layer = 0
        self.tgtcols = tgtcols
        self.tgttbl = tgttbl
        self.subquery = subquery

    def format(self) -> dict[str, Any]:
        return {
            "statement": "Insert",
            "layer": self.layer,
            "tgtcols": [col.format() for col in self.tgtcols],
            "tgttbl": self.tgttbl.format(),
            "subquery": self.subquery.format(),
        }


X: TypeAlias = Select | Insert
