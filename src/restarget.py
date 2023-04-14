from __future__ import annotations

from typing import Any

from column import Column
from table import Table


class ResTarget:
    def __init__(self, alias: str = "", refcols: list[Column] = []) -> None:
        self.alias = alias
        self.refcols = refcols

    def __str__(self) -> str:
        return str(self.format())

    def format(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "refcols": ", ".join([str(rc) for rc in self.refcols]),
        }

    def add_refcol(self, col: Column) -> None:
        self.refcols.append(col)

    def attach_table(self, table: Table) -> None:
        for rc in self.refcols:
            rc.set_table(table)
