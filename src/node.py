from __future__ import annotations

import abc
from typing import Any, Tuple

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
        tables: dict[str, table.Table] = {},
        layer: int = 0,
    ) -> None:
        self.columns = columns
        if tables:
            self.tables = tables
        else:
            set_tables = {
                refcol.table for refcols in columns.values() for refcol in refcols
            }
            self.tables = {st: table.Table(st) for st in set_tables}
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

    def _trace(self, column: str, results: list[Column]) -> None:
        for refcol in self.columns[column]:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table].ref, str):
                results.append(refcol)
            elif isinstance(self.tables[refcol.table].ref, Select):
                self.tables[refcol.table].ref._trace(refcol.name, results)

    def flatten(self) -> Select:
        f_columns: dict[str, list[Column]] = {}
        for column, refcols in self.columns.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in self.tables.keys():
                    raise Exception()
                if isinstance(self.tables[refcol.table].ref, str):
                    f_refcols.append(refcol)
                elif isinstance(self.tables[refcol.table].ref, Select):
                    self.tables[refcol.table].ref._trace(refcol.name, f_refcols)
            f_columns[column] = f_refcols
        return Select(f_columns)

    def summary(self) -> Tuple[dict[str, dict[str, set[str]]], list[str]]:
        flat = self.flatten()
        out_columns: list[str] = []
        in_tables: dict[str, dict[str, set[str]]] = {}
        for colname, refcols in flat.columns.items():
            out_columns.append(colname)
            for refcol in refcols:
                in_tables.setdefault(refcol.table, {})
                in_tables[refcol.table].setdefault(refcol.name, set())
                in_tables[refcol.table][refcol.name].add(colname)
        return in_tables, out_columns


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
