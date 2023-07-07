from __future__ import annotations

import abc
from typing import Any, NamedTuple, Tuple

from column import Column
from edge import ColEdge, TblEdge
from table import Table


class Summary(NamedTuple):
    src_tbls: dict[str, Table]
    tgt_tbl: dict[str, Table]
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

    def trace_table(self, results: list[str]) -> None:
        for tbl in self.tables.values():
            if isinstance(tbl, str):
                results.append(tbl)
            elif isinstance(tbl, Select):
                tbl.trace_table(results)
        return

    def summary(self, sqlnm: str) -> Summary:
        tgttbl_name = self.tgttblnm()

        tgt_tbl: dict[str, Table] = {tgttbl_name: Table(tgttbl_name)}
        src_tbls: dict[str, Table] = {}
        ref_tbls: set[str] = set()
        col_edges: set[ColEdge] = set()
        tbl_edges: set[TblEdge] = set()
        ref_edges: set[TblEdge] = set()

        tbl_edges.add(TblEdge(sqlnm, tgttbl_name))

        f = self._flatten()
        for colname, srccols in f.srccols.items():
            tgt_tbl[tgttbl_name].add(colname)
            for srccol in srccols:
                src_tbls.setdefault(srccol.table, Table(srccol.table))
                src_tbls[srccol.table].add(srccol)

                tail = srccol
                head = Column(tgttbl_name, colname)

                if tail != head:
                    col_edges.add(ColEdge(tail, head))

                if tail.table != tgttbl_name:
                    tbl_edges.add(TblEdge(tail.table, sqlnm))

                tbl_edges.add(TblEdge(sqlnm, head.table))

        for refcols in f.refcols.values():
            ref_tbls.update({rc.table for rc in refcols})

        for ft in f.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            if rt != tgttbl_name:
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

    def _trace_column(self, tgtcol: str, _type: int, results: list[Column]) -> None:
        cols = (
            self.srccols[tgtcol] if _type == 1 else self.refcols[tgtcol]
        )
        for refcol in cols:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table], str):
                results.append(Column(self.tables[refcol.table], refcol.name))
            elif isinstance(self.tables[refcol.table], Select):
                self.tables[refcol.table]._trace_column(refcol.name, _type, results)

    def __flatten_cols(self, _type: int) -> dict[str, list[Column]]:
        results: dict[str, list[Column]] = {}
        tgtcols = self.srccols if _type == 1 else self.refcols
        for column, srccols in tgtcols.items():
            f_srccols: list[Column] = []
            for refcol in srccols:
                if refcol.table not in self.tables:
                    raise Exception()
                elif isinstance(self.tables[refcol.table], str):
                    f_srccols.append(Column(self.tables[refcol.table], refcol.name))
                elif isinstance(self.tables[refcol.table], Select):
                    self.tables[refcol.table]._trace_column(
                        refcol.name, _type, f_srccols
                    )
            results[column] = f_srccols
        return results

    def _flatten(self) -> Select:
        f_srccols = self.__flatten_cols(1)
        f_refcols = self.__flatten_cols(2)
        refs: list[str] = []
        super().trace_table(refs)
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
        tgttable: str,
        tables: dict[str, str | Select] = {},
    ) -> None:
        self.srccols = srccols
        self.refcols = refcols
        self.tgttable = tgttable
        self.tables = tables

    def format(self) -> dict[str, Any]:
        return {
            "statement": Insert.STATEMENT,
            "tgtcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.tgtcols.items()
            },
            "tgttable": next(iter(self.tgttable)),
            "subquery": self.subquery.format() if self.subquery else "",
        }

    def tgttblnm(self) -> str:
        return self.tgttable

    def __flatten_cols(self, _type: int) -> dict[str, list[Column]]:
        results: dict[str, list[Column]] = {}
        tgtcols = self.srccols if _type == 1 else self.refcols
        for column, srccols in tgtcols.items():
            f_srccols: list[Column] = []
            for srccol in srccols:
                if srccol.table not in self.tables:
                    raise Exception()
                elif isinstance(self.tables[srccol.table], str):
                    f_srccols.append(Column(self.tables[srccol.table], srccol.name))
                elif isinstance(self.tables[srccol.table], Select):
                    self.tables[srccol.table]._trace_column(
                        srccol.name, _type, f_srccols
                    )
            results[column] = f_srccols
        return results

    def _flatten(self) -> Insert:
        f_srccols = self.__flatten_cols(1)
        f_refcols = self.__flatten_cols(2)
        refs: list[str] = []
        super().trace_table(refs)
        f_tables = {ref: ref for ref in refs}
        return Insert(f_srccols, f_refcols, self.tgttable, f_tables)

    def summary(self, sqlnm: str) -> Summary:
        return super().summary(sqlnm)


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

    def __flatten_cols(self, _type: int) -> dict[str, list[Column]]:
        results: dict[str, list[Column]] = {}
        tgtcols = self.srccols if _type == 1 else self.refcols
        for column, srccols in tgtcols.items():
            f_srccols: list[Column] = []
            for srccol in srccols:
                if (
                    srccol.table not in self.tables
                    and srccol.table not in self.tgttable
                ):
                    raise Exception()
                if srccol.table in self.tgttable:
                    f_srccols.append(Column(self.tgttable[srccol.table], srccol.name))
                else:
                    if isinstance(self.tables[srccol.table], str):
                        f_srccols.append(Column(self.tables[srccol.table], srccol.name))
                    elif isinstance(self.tables[srccol.table], Select):
                        self.tables[srccol.table]._trace_column(
                            srccol.name, _type, f_srccols
                        )
            results[column] = f_srccols
        return results

    def _flatten(self) -> Update:
        f_srccols = self.__flatten_cols(1)
        f_refcols = self.__flatten_cols(2)
        refs: list[str] = []
        super().trace_table(refs)
        f_tables = {ref: ref for ref in refs}
        return Update(f_srccols, f_refcols, self.tgttable, f_tables)

    def tgttblnm(self) -> str:
        return next(iter(self.tgttable.values()))

    def summary(self, sqlnm: str) -> Summary:
        return super().summary(sqlnm)


class Delete(Node):
    STATEMENT = "Delete"

    def __init__(
        self,
        tgttable: dict[str, str | Select],
        tables: dict[str, str | Select],
        srccols: dict[str, list[Column]] = {},
        refcols: dict[str, list[Column]] = {},
    ) -> None:
        self.tgttable = tgttable
        self.tables = tables
        self.srccols = srccols
        self.refcols = refcols

    def format(self) -> dict[str, Any]:
        return {}

    def _flatten(self) -> Delete:
        refs: list[str] = []
        super().trace_table(refs)
        f_tables = {ref: ref for ref in refs}
        return Delete(self.tgttable, f_tables)

    def tgttblnm(self) -> str:
        return next(iter(self.tgttable.values()))

    def summary(self, sqlnm: str) -> Summary:
        return super().summary(sqlnm)
