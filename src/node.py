from __future__ import annotations

import abc
from typing import Any

import table
from column import Column
from restarget import ResTarget


class Node(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def format(self) -> dict[str, Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def flatten(self) -> dict[str, Any]:
        raise NotImplementedError()


class Select(Node):
    STATEMENT = "Select"

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
            "statement": Select.STATEMENT,
            "layer": self.layer,
            "columns": {
                (str(restgt) if str(restgt) else "column-" + str(i)): [
                    rc for rc in restgt.refcols
                ]
                for i, restgt in enumerate(self.columns)
            },
            "tables": {str(t): t for t in self.tables},
        }

    @classmethod
    def trace(
        cls, node: dict[str, Any], refcols: dict[str, Any], ansestors: dict[str, Any]
    ):
        if not node["layer"]:
            refcols = node["columns"]

        nexts_refcols: dict[str, Any] = {}
        for colnm, cols in refcols.items():
            nexts_refcols[colnm] = []
            for col in cols:
                if [
                    tbl
                    for tbl in node["tables"]
                    if isinstance(tbl, str) and tbl == col.table
                ]:
                    ansestors[colnm].append(col)
                else:
                    nexts_refcols[colnm].append(col)

        next_nodes = [tbl for tbl in node["tables"] if isinstance(tbl, dict)]
        if nexts_refcols and next_nodes:
            cls.trace()

    def flatten(self) -> dict[str, Any]:
        targets: list[list[Column]]
        refs: list[Column]
        return {"targets": targets, "reds": refs}


class Insert(Node):
    STATEMENT = "Insert"

    def __init__(
        self,
        tgtcols: list[ResTarget],
        tgttbl: "table.Table",
        subquery: Select,
    ) -> None:
        self.layer = 0
        self.tgtcols = tgtcols
        self.tgttable = tgttbl
        self.subquery = subquery

    def format(self) -> dict[str, Any]:
        return {
            "statement": Insert.STATEMENT,
            "layer": self.layer,
            "tgtcols": [col.format() for col in self.tgtcols],
            "tgttbl": self.tgttable.format(),
            "subquery": self.subquery.format(),
        }

    def flatten(self) -> dict[str, Any]:
        dst_table: str
        dst_cols: dict[str, list[Column]]
        refs: list[Column]
        return {"dst_table": dst_table, "dst_cols": dst_cols, "reds": refs}
