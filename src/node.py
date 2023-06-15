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
        srccols: dict[str, list[Column]],
        refcols: dict[str, list[Column]],
        tables: dict[str, table.Table] = {},
        layer: int = 0,
        name: str = "",
    ) -> None:
        self.srccols = srccols
        self.refcols = refcols
        self.tables = tables
        self.layer = layer
        self.name = name

    def format(self) -> dict[str, Any]:
        return {
            "statement": Select.STATEMENT,
            "layer": self.layer,
            "srccols": {
                colnm: [str(sc) for sc in srccols]
                for colnm, srccols in self.srccols.items()
            },
            "refcols": {
                colnm: [str(rc) for rc in refcols]
                for colnm, refcols in self.refcols.items()
            },
            "tables": {tblnm: tbl.format() for tblnm, tbl in self.tables.items()},
            "name": self.name,
        }

    def _trace_column(self, column: str, results: list[Column]) -> None:
        for refcol in self.srccols[column]:
            if refcol.table not in self.tables.keys():
                raise Exception()
            if isinstance(self.tables[refcol.table].ref, str):
                results.append(Column(self.tables[refcol.table].ref, refcol.name))
            elif isinstance(self.tables[refcol.table].ref, Select):
                self.tables[refcol.table].ref._trace_column(refcol.name, results)

    def _trace_table(self, results: list[str]) -> None:
        for tbl in self.tables.values():
            if isinstance(tbl.ref, str):
                results.append(tbl.ref)
            elif isinstance(tbl.ref, Select):
                tbl.ref._trace_table(results)
        return

    def _aa(self, columns: dict[str, list[Column]]) -> dict[str, list[Column]]:
        f_columns: dict[str, list[Column]] = {}
        for column, refcols in columns.items():
            f_refcols: list[Column] = []
            for refcol in refcols:
                if refcol.table not in self.tables.keys():
                    raise Exception()
                if isinstance(self.tables[refcol.table].ref, str):
                    f_refcols.append(Column(self.tables[refcol.table].ref, refcol.name))
                elif isinstance(self.tables[refcol.table].ref, Select):
                    self.tables[refcol.table].ref._trace_column(refcol.name, f_refcols)
            f_columns[column] = f_refcols
            if column == "column-1":
                for rc in f_refcols:
                    print()
                    print(rc)
                    print("cc")

        return f_columns

    def _flatten(self) -> Select:
        f_srccols = self._aa(self.srccols)
        f_refcols = self._aa(self.refcols)
        for k, v in f_refcols.items():
            print()
            print("ff")
            for rc in v:
                print(k)
                print(rc)
                print("dd")
                print()
        refs: list[str] = []
        self._trace_table(refs)
        f_tables = {ref: table.Table(ref) for ref in refs}
        return Select(f_srccols, f_refcols, f_tables, name=self.name)

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
        for colname, srccols in f.srccols.items():
            tgt_tbl[out_tblnm].setdefault(colname)
            for srccol in srccols:
                src_tbls.setdefault(srccol.table, {})
                src_tbls[srccol.table].setdefault(srccol.name)

                from_ = srccol
                to = Column(out_tblnm, colname)
                col_edges.setdefault(str(from_) + str(to), (from_, to))
                tbl_edges.setdefault(
                    str(from_.table) + self.name, (from_.table, self.name)
                )
                tbl_edges.setdefault(self.name + str(to.table), (self.name, to.table))

        for refcols in f.refcols.values():
            ref_tbls.update({rc.table for rc in refcols})

        for ft in f.tables:
            if ft not in src_tbls.keys():
                ref_tbls.add(ft)

        for rt in ref_tbls:
            key = rt + self.name
            ref_edges.setdefault(key, (rt, self.name))

        tbl_edges.setdefault(self.name + out_tblnm, (self.name, out_tblnm))

        return Summary(src_tbls, tgt_tbl, ref_tbls, col_edges, tbl_edges, ref_edges)


class Insert(Node):
    STATEMENT = "Insert"

    def __init__(
        self,
        tgtcols: dict[str, list[Column]],
        tgttable: dict[str, table.Table],
        subquery: Select | None,
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
                f_refcols = subquery.tables[refcol.table].ref.srccols[refcol.name]
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
        for tgtcol, srccol in zip(f.tgtcols, f.subquery.srccols):
            tgt_tbl[tgttbl_name].setdefault(tgtcol, None)

            for refcol in f.subquery.srccols[srccol]:
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
                results.append(Column(self.tables[refcol.table].ref, refcol.name))
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
        f_tables: dict[str, table.Table] = {}

        for column, refcols in self.tgtcols.items():
            if isinstance(refcols, Select):
                f = refcols._flatten()
                f_tgtcols[column] = next(iter(f.srccols.values()))
                f_tables.update(f.tables)
                f_tables.update(self.tables)

            elif isinstance(refcols, list):
                if self.tables:
                    f_refcols: list[Column] = []
                    for refcol in refcols:
                        if refcol.table not in self.tables.keys():
                            raise Exception()
                        if isinstance(self.tables[refcol.table].ref, str):
                            f_refcols.append(
                                Column(self.tables[refcol.table].ref, refcol.name)
                            )
                        elif isinstance(self.tables[refcol.table].ref, Select):
                            self.tables[refcol.table].ref._trace_column(
                                refcol.name, f_refcols
                            )
                    f_tgtcols[column] = f_refcols
                else:
                    f_tgtcols[column] = refcols

                refs: list[str] = []
                self._trace_table(refs)
                f_tables.update({ref: table.Table(ref) for ref in refs})

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
