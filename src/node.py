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
    def _flatten(self) -> Node:
        raise NotImplementedError()

    @abc.abstractmethod
    def summary(
        self,
    ) -> Tuple[
        dict[str, dict[str, None]],
        dict[str, dict[str, None]],
        set[str],
        dict[str, Tuple[Column, Column]],
        dict[str, Tuple[str, str]],
        dict[str, Tuple[str, str]],
    ]:
        raise NotImplementedError()


class Select(Node):
    STATEMENT = "Select"
    __COUNT = 0

    def __init__(
        self,
        columns: dict[str, list[Column]],
        tables: dict[str, table.Table] = {},
        layer: int = 0,
        name: str = "",
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
        self.name = name

    @staticmethod
    def empty() -> Select:
        return Select({}, {}, -1, "")

    def format(self) -> dict[str, Any]:
        return {
            "statement": Select.STATEMENT,
            "layer": self.layer,
            "columns": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.columns.items()
            },
            "tables": {tblnm: tbl.format() for tblnm, tbl in self.tables.items()},
            "name": self.name,
        }

    def _trace_column(self, column: str, results: list[Column]) -> None:
        for refcol in self.columns[column]:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table].ref, str):
                results.append(refcol)
            elif isinstance(self.tables[refcol.table].ref, Select):
                self.tables[refcol.table].ref._trace_column(refcol.name, results)

    def _trace_table(self, results: list[str]) -> None:
        for tbl in self.tables.values():
            if isinstance(tbl.ref, str):
                results.append(tbl.ref)
            elif isinstance(tbl.ref, Select):
                tbl.ref._trace_table(results)
        return

    def _flatten(self) -> Select:
        f_columns: dict[str, list[Column]] = {}
        for column, refcols in self.columns.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in self.tables.keys():
                    raise Exception()
                if isinstance(self.tables[refcol.table].ref, str):
                    f_refcols.append(refcol)
                elif isinstance(self.tables[refcol.table].ref, Select):
                    self.tables[refcol.table].ref._trace_column(refcol.name, f_refcols)
            f_columns[column] = f_refcols

        refs: list[str] = []
        self._trace_table(refs)
        f_tables = {ref: ref for ref in refs}

        return Select(f_columns, f_tables, name=self.name)

    def summary(
        self,
    ) -> Tuple[
        dict[str, dict[str, None]],
        dict[str, dict[str, None]],
        set[str],
        dict[str, Tuple[Column, Column]],
        dict[str, Tuple[str, str]],
        dict[str, Tuple[str, str]],
    ]:
        out_tblnm = Select.STATEMENT + "-" + str(Select.__COUNT)
        Select.__COUNT += 1

        tgt_tbl: dict[str, dict[str, None]] = {out_tblnm: {}}
        src_tbls: dict[str, dict[str, None]] = {}
        ref_tbls: set[str] = set()
        col_flows: dict[str, Tuple[Column, Column]] = {}
        tbl_flows: dict[str, Tuple[str, str]] = {}
        ref_edges: dict[str, Tuple[str, str]] = {}

        f = self._flatten()
        for colname, refcols in f.columns.items():
            tgt_tbl[out_tblnm].setdefault(colname)
            for refcol in refcols:
                src_tbls.setdefault(refcol.table, {})
                src_tbls[refcol.table].setdefault(refcol.name)

                from_ = refcol
                to = Column(out_tblnm, colname)
                col_flows.setdefault(str(from_) + str(to), (from_, to))
                tbl_flows.setdefault(
                    str(from_.table) + self.name, (from_.table, self.name)
                )
                tbl_flows.setdefault(self.name + str(to.table), (self.name, to.table))

        for ft in f.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            key = rt + self.name
            ref_edges.setdefault(key, (rt, self.name))

        return src_tbls, tgt_tbl, ref_tbls, col_flows, tbl_flows, ref_edges


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

    def _flatten(self) -> dict[str, Any]:
        dst_table: str
        dst_cols: dict[str, list[Column]]
        refs: list[Column]
        return {"dst_table": dst_table, "dst_cols": dst_cols, "reds": refs}
