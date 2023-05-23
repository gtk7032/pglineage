from __future__ import annotations

import abc
from typing import Any

import table
from column import Column


class Node(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def format(self) -> dict[str, Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def flatten(self) -> Node:
        raise NotImplementedError()


class Select(Node):
    STATEMENT = "Select"

    def __init__(
        self,
        columns: dict[str, list[Column]],
        tables: dict[str, "table.Table"],
        layer: int = 0,
    ) -> None:
        self.columns = columns
        self.tables = tables
        self.layer = layer

    @staticmethod
    def empty() -> Select:
        return Select({}, {}, -1)

    def format(self) -> dict[str, Any]:
        return {
            "statement": Select.STATEMENT,
            "layer": self.layer,
            "columns": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.columns.items()
            },
            "tables": {tblnm: tbl.format() for tblnm, tbl in self.tables.items()},
        }

    def trace(self, column: str, results: list[Column]) -> None:
        for refcol in self.columns[column]:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table].ref, str):
                results.append(refcol)
            elif isinstance(self.tables[refcol.table].ref, Select):
                self.tables[refcol.table].ref.trace(refcol.name, results)
            else:
                raise Exception()

    def flatten(self) -> dict[str, list[Column]]:
        f_columns: dict[str, list[Column]] = {}
        for column, refcols in self.columns.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if isinstance(self.tables[refcol.table].ref, str):
                    f_refcols.append(refcol)
                elif isinstance(self.tables[refcol.table].ref, Select):
                    self.tables[refcol.table].ref.trace(column, f_refcols)
            f_columns[column] = f_refcols
        return f_columns

    # @classmethod
    # def trace(
    #     cls, node: dict[str, Any], refcols: dict[str, Any], ansestors: dict[str, Any]
    # ):
    #     if not node["layer"]:
    #         refcols = node["columns"]

    #     nexts_refcols: dict[str, Any] = {}
    #     for colnm, cols in refcols.items():
    #         nexts_refcols[colnm] = []
    #         for col in cols:
    #             if [
    #                 tbl
    #                 for tbl in node["tables"]
    #                 if isinstance(tbl, str) and tbl == col.table
    #             ]:
    #                 ansestors[colnm].append(col)
    #             else:
    #                 nexts_refcols[colnm].append(col)

    #     next_nodes = [tbl for tbl in node["tables"] if isinstance(tbl, dict)]
    #     if nexts_refcols and next_nodes:
    #         cls.trace()

    def flatten(self) -> dict[str, Any]:
        # targets: list[list[Column]]
        # refs: list[Column]
        return {}


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
