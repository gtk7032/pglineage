from __future__ import annotations

import abc
from typing import Any, NamedTuple, Tuple

from column import Column
from edge import ColEdge, TblEdge


class Summary(NamedTuple):
    src_tbls: dict[str, dict[str, None]]
    tgt_tbl: dict[str, dict[str, None]]
    ref_tbls: set[str]
    col_edges: set[ColEdge]
    tbl_edges: set[TblEdge]
    ref_edges: set[TblEdge]


class Node(metaclass=abc.ABCMeta):
    STATEMENT = ""

    @abc.abstractmethod
    def format(self) -> dict[str, Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def _flatten(self) -> Node:
        raise NotImplementedError()

    @abc.abstractmethod
    def tgttblnm(self) -> str:
        raise NotImplementedError()

    def summary(self, sqlnm: str) -> Summary:
        tgttbl_name = self.tgttblnm()

        tgt_tbl: dict[str, dict[str, None]] = {tgttbl_name: {}}
        src_tbls: dict[str, dict[str, None]] = {}
        ref_tbls: set[str] = set()
        col_edges: set[ColEdge] = set()
        tbl_edges: set[TblEdge] = set()
        ref_edges: set[TblEdge] = set()

        tbl_edges.add(TblEdge(sqlnm, tgttbl_name))

        f = self._flatten()
        for colname, srccols in f.srccols.items():
            tgt_tbl[tgttbl_name].setdefault(colname, None)
            for srccol in srccols:
                src_tbls.setdefault(srccol.table, {})
                src_tbls[srccol.table].setdefault(srccol.name)

                tail = srccol
                head = Column(tgttbl_name, colname)

                col_edges.add(ColEdge(tail, head))
                tbl_edges.add(TblEdge(tail.table, sqlnm))
                tbl_edges.add(TblEdge(sqlnm, head.table))

        for refcols in f.refcols.values():
            ref_tbls.update({rc.table for rc in refcols})

        for ft in f.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            ref_edges.add(TblEdge(rt, sqlnm))

        tbl_edges.add(TblEdge(sqlnm, tgttbl_name))

        return Summary(src_tbls, tgt_tbl, ref_tbls, col_edges, tbl_edges, ref_edges)


class Select(Node):
    STATEMENT = "Select"
    __COUNT = 0

    def __init__(
        self,
        srccols: dict[str, list[Column]],
        refcols: dict[str, list[Column]],
        tables: dict[str, str | Select] = {},
    ) -> None:
        self.srccols = srccols
        self.refcols = refcols
        self.tables = tables

    def format(self) -> dict[str, Any]:
        return {
            "statement": Select.STATEMENT,
            "srccols": {
                colnm: [str(sc) for sc in srccols]
                for colnm, srccols in self.srccols.items()
            },
            "refcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.refcols.items()
            },
            "tables": {tblnm: tbl.format() for tblnm, tbl in self.tables.items()},
        }

    def _trace_column(self, column: str, results: list[Column]) -> None:
        for refcol in self.srccols[column]:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table], str):
                results.append(Column(self.tables[refcol.table], refcol.name))
            elif isinstance(self.tables[refcol.table], Select):
                self.tables[refcol.table]._trace_column(refcol.name, results)

    def _trace_table(self, results: list[str]) -> None:
        for tbl in self.tables.values():
            if isinstance(tbl, str):
                results.append(tbl)
            elif isinstance(tbl, Select):
                tbl._trace_table(results)
        return

    def __flatten_srccols(
        self, columns: dict[str, list[Column]]
    ) -> dict[str, list[Column]]:
        f_columns: dict[str, list[Column]] = {}
        for column, refcols in columns.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in self.tables:
                    raise Exception()
                elif isinstance(self.tables[refcol.table], str):
                    f_refcols.append(Column(self.tables[refcol.table], refcol.name))
                elif isinstance(self.tables[refcol.table], Select):
                    self.tables[refcol.table]._trace_column(refcol.name, f_refcols)
            f_columns[column] = f_refcols
        return f_columns

    def _flatten(self) -> Select:
        f_srccols = self.__flatten_srccols(self.srccols)
        f_refcols = self.refcols
        refs: list[str] = []
        self._trace_table(refs)
        f_tables = {ref: ref for ref in refs}
        return Select(f_srccols, f_refcols, f_tables)

    def tgttblnm(self) -> str:
        return self.__class__.STATEMENT + "-" + str(self.__class__.__COUNT)

    def summary(self, sqlnm: str) -> Summary:
        return super().summary(sqlnm)


class Insert(Node):
    STATEMENT = "Insert"

    def __init__(
        self,
        srccols: dict[str, list[Column]],
        refcols: dict[str, list[Column]],
        tgttable: dict[str, str | Select],
        subquery: Select | None,
    ) -> None:
        self.srccols = srccols
        self.refcols = refcols
        self.tgttable = tgttable
        self.subquery = subquery

    def format(self) -> dict[str, Any]:
        return {
            "statement": Insert.STATEMENT,
            "tgtcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.srccols.items()
            },
            "tgttable": next(iter(self.tgttable)),
            "subquery": self.subquery.format() if self.subquery else "",
        }

    def _flatten(self) -> Insert:
        if not self.subquery:
            return self

        subquery = self.subquery._flatten()

        f_tgtcols: dict[str, list[Column]] = {}
        for column, refcols in self.srccols.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in subquery.tables.keys():
                    raise Exception()
                f_refcols = subquery.tables[refcol.table].srccols[refcol.name]
            f_tgtcols[column] = f_refcols

        return Insert(f_tgtcols, self.tgttable, subquery)

    def summary(self, sqlnm: str) -> Summary:
        tgttbl_name: str = next(iter(self.tgttable.values()))
        tgt_tbl: dict[str, dict[str, None]] = {tgttbl_name: {}}
        src_tbls: dict[str, dict[str, None]] = {}
        ref_tbls: set[str] = set()
        col_edges: dict[str, Tuple[Column, Column]] = {}
        tbl_edges: dict[str, Tuple[str, str]] = {}
        ref_edges: dict[str, Tuple[str, str]] = {}

        f = self._flatten()
        for tgtcol, srccol in zip(f.srccols, f.subquery.srccols):
            tgt_tbl[tgttbl_name].setdefault(tgtcol, None)

            for refcol in f.subquery.srccols[srccol]:
                from_ = refcol
                to = Column(tgttbl_name, tgtcol)

                src_tbls.setdefault(refcol.table, {})
                src_tbls[refcol.table].setdefault(refcol.name)

                col_edges.setdefault(str(from_) + str(to), (from_, to))
                tbl_edges.setdefault(str(from_.table) + sqlnm, (from_.table, sqlnm))
                tbl_edges.setdefault(sqlnm + str(to.table), (sqlnm, to.table))

        for ft in f.subquery.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            key = rt + sqlnm
            ref_edges.setdefault(key, (rt, sqlnm))

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
        srccols: dict[str, list[Column]],
        refcols: dict[str, list[Column]],
        tgttable: dict[str, str | Select],
        tables: dict[str, str | Select] = {},
    ) -> None:
        self.srccols = srccols
        self.refcols = refcols
        self.tgttable = tgttable
        self.tables = tables

    def format(self) -> dict[str, Any]:
        return {
            "statement": Update.STATEMENT,
            "tgtcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.srccols.items()
            },
            "tgttable": next(iter(self.tgttable)),
            "tables": {tblnm: tbl.format() for tblnm, tbl in self.tables.items()},
        }

    def _trace_column(self, column: str, results: list[Column]) -> None:
        for refcol in self.srccols[column]:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table], str):
                results.append(Column(self.tables[refcol.table], refcol.name))
            elif isinstance(self.tables[refcol.table], Select):
                self.tables[refcol.table]._trace_column(refcol.name, results)

    def _trace_table(self, results: list[str]) -> None:
        for tbl in self.tables.values():
            if isinstance(tbl, str):
                results.append(tbl)
            elif isinstance(tbl, Select):
                tbl._trace_table(results)
        return

    def __aa(self, columns: dict[str, list[Column]]) -> dict[str, list[Column]]:
        pass

    def _flatten(self) -> Update:
        f_tgtcols: dict[str, list[Column]] = {}
        f_tables: dict[str, str | Select] = {}

        for column, refcols in self.srccols.items():
            if self.tables:
                f_refcols: list[Column] = []
                for refcol in refcols:
                    if refcol.table not in self.tables.keys():
                        raise Exception()
                    if isinstance(self.tables[refcol.table], str):
                        f_refcols.append(Column(self.tables[refcol.table], refcol.name))
                    elif isinstance(self.tables[refcol.table], Select):
                        self.tables[refcol.table]._trace_column(refcol.name, f_refcols)
                f_tgtcols[column] = f_refcols
            else:
                f_tgtcols[column] = refcols

            refs: list[str] = []
            self._trace_table(refs)
            f_tables.update({ref: ref for ref in refs})

        return Update(f_tgtcols, self.tgttable, f_tables)

    def tgttblnm(self) -> str:
        return next(iter(self.tgttable.values()))

    def summary(self, sqlnm: str) -> Summary:
        return super().summary(sqlnm)
