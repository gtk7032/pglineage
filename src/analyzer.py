from typing import Any

from pglast import ast, parse_sql

import node
from column import Column
from table import Table


class Analyzer:
    def __init__(self) -> None:
        self.__rawstmts: list[Any] = []

    def load(self, sqls: str) -> None:
        self.__rawstmts.extend(sql.stmt for sql in parse_sql(sqls))

    def analyze(self) -> list[node.Node]:
        nodes: list[node.Node] = []
        for rawstmt in self.__rawstmts:
            if isinstance(rawstmt, ast.SelectStmt):
                nodes.append(self._analyze_select(rawstmt(skip_none=True)))
            elif isinstance(rawstmt, ast.InsertStmt):
                nodes.append(self._analyze_insert(rawstmt(skip_none=True)))
        return nodes

    def _analyze_fromclause(
        self, fc: dict[str, Any], tables: dict[str, Table], layer: int = 0
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables[fc["alias"]["aliasname"]] = Table(
                self._analyze_select(fc["subquery"], layer + 1),
            )

        elif fc["@"] == "RangeVar":
            tblnm = fc["alias"]["aliasname"] if "alias" in fc.keys() else fc["relname"]
            tables.setdefault(tblnm, Table(fc["relname"]))

        for v in fc.values():
            if isinstance(v, dict):
                self._analyze_fromclause(v, tables, layer)

    def _analyze_restargets(
        self, restargets: list[dict[str, Any]]
    ) -> dict[str, list[Column]]:
        results: dict[str, list[Column]] = {}

        for i, tgt in enumerate(restargets):
            if tgt.get("@", "") != "ResTarget":
                Exception()

            refcols: list[Column] = []

            for v in tgt.values():
                if isinstance(v, tuple):
                    for vv in v:
                        self._extract_refcols(vv, refcols)
                else:
                    self._extract_refcols(v, refcols)

            name = tgt.get(
                "name", refcols[0].name if len(refcols) == 1 else "column-" + str(i)
            )
            results[name] = refcols

        return results

    def _extract_refcols(self, tgt: dict[str, Any], refcols: list[Column]) -> None:
        if not isinstance(tgt, dict):
            return

        if tgt.get("@", "") == "ColumnRef" and "fields" in tgt.keys():
            refcol = []
            for field in tgt["fields"]:
                if "sval" in field.keys():
                    refcol.append(field["sval"])
            if refcol:
                refcols.append(Column.create_from_list(refcol))
            return

        for v in tgt.values():
            if isinstance(v, tuple):
                for vv in v:
                    self._extract_refcols(vv, refcols)
            else:
                self._extract_refcols(v, refcols)

    def _analyze_select(
        self, statement: dict[str, Any], layer: int = 0
    ) -> node.Select:
        columns = self._analyze_restargets(statement["targetList"])
        tables: dict[str, Table] = {}

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables[cte["ctename"]] = Table(
                    self._analyze_select(cte["ctequery"], layer + 1)
                )

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self._analyze_fromclause(fc, tables, layer)

        if len(tables.keys()) == 1:
            for refcols in columns.values():
                for rc in refcols:
                    rc.set_table(next(iter(tables)))

        return node.Select(columns, tables, layer)

    def _analyze_insert(self, stmt: dict[str, Any]) -> node.Insert:
        res = self._analyze_restargets(stmt["cols"])
        rel = stmt["relation"]
        tbl = Table(
            rel["alias"]["aliasname"] if "alias" in rel.keys() else "", rel["relname"]
        )
        select = (
            self._analyze_select(stmt["selectStmt"], 1)
            if "selectStmt" in stmt.keys()
            else node.Select.empty()
        )
        return node.Insert(res, tbl, select)
