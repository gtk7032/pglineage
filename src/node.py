from __future__ import annotations

import abc
from typing import Any, NamedTuple, Tuple

import table
from column import Column


class Summary(NamedTuple):
    src_tbls: dict[str, dict[str, None]]
    tgt_tbl: dict[str, dict[str, None]]
    ref_tbls: set[str]
    col_edges: dict[str, Tuple[Column, Column]]
    tbl_edges: dict[str, Tuple[str, str]]
    ref_edges: dict[str, Tuple[str, str]]


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
    ) -> Summary:
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
        self.tables = tables
        self.layer = layer
        self.name = name

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
        f_tables = {ref: table.Table(ref) for ref in refs}

        return Select(f_columns, f_tables, name=self.name)

    def summary(
        self,
    ) -> Summary:
        out_tblnm = Select.STATEMENT + "-" + str(Select.__COUNT)
        Select.__COUNT += 1

        tgt_tbl: dict[str, dict[str, None]] = {out_tblnm: {}}
        src_tbls: dict[str, dict[str, None]] = {}
        ref_tbls: set[str] = set()
        col_edges: dict[str, Tuple[Column, Column]] = {}
        tbl_edges: dict[str, Tuple[str, str]] = {}
        ref_edges: dict[str, Tuple[str, str]] = {}

        f = self._flatten()
        for colname, refcols in f.columns.items():
            tgt_tbl[out_tblnm].setdefault(colname)
            for refcol in refcols:
                src_tbls.setdefault(refcol.table, {})
                src_tbls[refcol.table].setdefault(refcol.name)

                from_ = refcol
                to = Column(out_tblnm, colname)
                col_edges.setdefault(str(from_) + str(to), (from_, to))
                tbl_edges.setdefault(
                    str(from_.table) + self.name, (from_.table, self.name)
                )
                tbl_edges.setdefault(self.name + str(to.table), (self.name, to.table))

        for ft in f.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            key = rt + self.name
            ref_edges.setdefault(key, (rt, self.name))

        return Summary(src_tbls, tgt_tbl, ref_tbls, col_edges, tbl_edges, ref_edges)


class Insert(Node):
    STATEMENT = "Insert"

    def __init__(
        self,
        tgtcols: dict[str, list[Column]],
        tgttable: dict[str, table.Table],
        subquery: Select,
        layer: int = 0,
        name: str = "",
    ) -> None:
        self.tgtcols = tgtcols
        self.tgttable = tgttable
        self.subquery = subquery
        self.layer = layer
        self.name = name

    def format(self) -> dict[str, Any]:
        return {
            "statement": Insert.STATEMENT,
            "layer": self.layer,
            "tgtcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.tgtcols.items()
            },
            "tgttable": next(iter(self.tgttable)),
            "subquery": self.subquery.format() if self.subquery else "",
            "name": self.name,
        }

    def _flatten(self) -> Insert:
        if not self.subquery:
            return self

        subquery = self.subquery._flatten()

        f_tgtcols: dict[str, list[Column]] = {}
        for column, refcols in self.tgtcols.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in subquery.tables.keys():
                    raise Exception()
                f_refcols = subquery.tables[refcol.table].ref.columns[refcol.name]
            f_tgtcols[column] = f_refcols

        return Insert(f_tgtcols, self.tgttable, subquery, 0, self.name)

    def summary(
        self,
    ) -> Summary:
        tgttbl_name = next(iter(self.tgttable.values())).ref
        tgt_tbl: dict[str, dict[str, None]] = {tgttbl_name: {}}
        src_tbls: dict[str, dict[str, None]] = {}
        ref_tbls: set[str] = set()
        col_edges: dict[str, Tuple[Column, Column]] = {}
        tbl_edges: dict[str, Tuple[str, str]] = {}
        ref_edges: dict[str, Tuple[str, str]] = {}

        f = self._flatten()
        for tgtcol, srccol in zip(f.tgtcols, f.subquery.columns):
            tgt_tbl[tgttbl_name].setdefault(tgtcol, None)

            for refcol in f.subquery.columns[srccol]:
                from_ = refcol
                to = Column(tgttbl_name, tgtcol)

                src_tbls.setdefault(refcol.table, {})
                src_tbls[refcol.table].setdefault(refcol.name)

                col_edges.setdefault(str(from_) + str(to), (from_, to))
                tbl_edges.setdefault(str(from_.table) + f.name, (from_.table, f.name))
                tbl_edges.setdefault(f.name + str(to.table), (f.name, to.table))

        for ft in f.subquery.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            key = rt + self.name
            ref_edges.setdefault(key, (rt, self.name))

        return Summary(
            src_tbls=src_tbls,
            tgt_tbl=tgt_tbl,
            ref_tbls=ref_tbls,
            col_edges=col_edges,
            tbl_edges=tbl_edges,
            ref_edges=ref_edges,
        )


class Update(Node):
    STATEMENT = "Update"

    def __init__(
        self,
        tgtcols: dict[str, list[Column]],
        tgttable: dict[str, table.Table],
        tables: dict[str, table.Table] = {},
        layer: int = 0,
        name: str = "",
    ) -> None:
        self.tgtcols = tgtcols
        self.tgttable = tgttable
        self.tables = tables
        self.layer = layer
        self.name = name

    def format(self) -> dict[str, Any]:
        return {
            "statement": Update.STATEMENT,
            "layer": self.layer,
            "tgtcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.tgtcols.items()
            },
            "tgttable": next(iter(self.tgttable)),
            "tables": {tblnm: tbl.format() for tblnm, tbl in self.tables.items()},
            "name": self.name,
        }

    def _trace_column(self, column: str, results: list[Column]) -> None:
        for refcol in self.tgtcols[column]:
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

    def _flatten(self) -> Update:
        f_tgtcols: dict[str, list[Column]] = {}
        for column, refcols in self.tgtcols.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in self.tables.keys():
                    raise Exception()
                if isinstance(self.tables[refcol.table].ref, str):
                    f_refcols.append(refcol)
                elif isinstance(self.tables[refcol.table].ref, Select):
                    self.tables[refcol.table].ref._trace_column(refcol.name, f_refcols)
            f_tgtcols[column] = f_refcols

            refs: list[str] = []
            self._trace_table(refs)
            f_tables = {ref: table.Table(ref) for ref in refs}

        return Update(f_tgtcols, self.tgttable, f_tables, 0, self.name)

    def summary(
        self,
    ) -> Summary:
        tgttbl_name = next(iter(self.tgttable.values())).ref
        tgt_tbl: dict[str, dict[str, None]] = {tgttbl_name: {}}
        src_tbls: dict[str, dict[str, None]] = {}
        ref_tbls: set[str] = set()
        col_edges: dict[str, Tuple[Column, Column]] = {}
        tbl_edges: dict[str, Tuple[str, str]] = {}
        ref_edges: dict[str, Tuple[str, str]] = {}

        f = self._flatten()
        for tgtcol, refcols in f.tgtcols.items():
            tgt_tbl[tgttbl_name].setdefault(tgtcol, None)
            for refcol in refcols:
                src_tbls.setdefault(refcol.table, {})
                src_tbls[refcol.table].setdefault(refcol.name)

                from_ = refcol
                to = Column(tgttbl_name, tgtcol)

                col_edges.setdefault(str(from_) + str(to), (from_, to))
                tbl_edges.setdefault(str(from_.table) + f.name, (from_.table, f.name))
                tbl_edges.setdefault(f.name + str(to.table), (f.name, to.table))

        for ft in f.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            key = rt + self.name
            ref_edges.setdefault(key, (rt, self.name))

        return Summary(
            src_tbls=src_tbls,
            tgt_tbl=tgt_tbl,
            ref_tbls=ref_tbls,
            col_edges=col_edges,
            tbl_edges=tbl_edges,
            ref_edges=ref_edges,
        )
