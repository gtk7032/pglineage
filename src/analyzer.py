from typing import Any, Tuple

from pglast import ast, parse_sql

import node
from column import Column
from table import Table


class Analyzer:
    def __init__(self) -> None:
        self.__rawstmts: list[Tuple[str, Any]] = []

    def load(self, sqls: str, name: str) -> None:
        self.__rawstmts.extend(
            (name.lower(), sql.stmt) for sql in parse_sql(sqls.lower())
        )

    def index(self) -> None:
        tmp = []
        for stmt1 in self.__rawstmts:
            cnt = 0
            for stmt2 in self.__rawstmts:
                if stmt1 == stmt2:
                    break
                if stmt2[0].startswith(stmt1[0]):
                    cnt += 1
            if cnt:
                tmp.append((stmt1[0] + "-" + str(cnt + 1), stmt1[1]))
            else:
                tmp.append((stmt1[0], stmt1[1]))
        self.__rawstmts = tmp

    def analyze(self) -> list[node.Node]:
        self.index()
        nodes: list[node.Node] = []
        for name, rawstmt in self.__rawstmts:
            match rawstmt:
                case ast.SelectStmt():
                    analyze_stmt = self._analyze_select
                case ast.InsertStmt():
                    analyze_stmt = self._analyze_insert
                case ast.UpdateStmt():
                    analyze_stmt = self._analyze_update
            nodes.append(analyze_stmt(rawstmt(skip_none=True), name=name))
        return nodes

    def _analyze_fromclause(
        self,
        fc: dict[str, Any],
        tables: dict[str, Table],
        layer: int = 0,
        name: str = "",
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables[fc["alias"]["aliasname"]] = Table(
                self._analyze_select(fc["subquery"], layer + 1, name=name),
            )

        elif fc["@"] == "RangeVar":
            tblnm = fc["alias"]["aliasname"] if "alias" in fc.keys() else fc["relname"]
            tables.setdefault(tblnm, Table(fc["relname"]))

        for v in fc.values():
            if isinstance(v, dict):
                self._analyze_fromclause(v, tables, layer, name)

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
                "name", refcols[0].name if len(refcols) == 1 else "column-" + str(i + 1)
            )
            ls = [nm for nm in results.keys() if nm.startswith(name)]
            name += "(" + str(len(ls) + 1) + ")" if len(ls) else ""
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

        if tgt.get("@", "") == "SelectStmt":
            subquery = self._analyze_select(tgt["SelectStmt"])
            refcols = next(iter(subquery._flatten().columns.values()))
            return

        for v in tgt.values():
            if isinstance(v, tuple):
                for vv in v:
                    self._extract_refcols(vv, refcols)
            else:
                self._extract_refcols(v, refcols)

    def _analyze_select(
        self, statement: dict[str, Any], layer: int = 0, name: str = ""
    ) -> node.Select:
        columns = self._analyze_restargets(statement["targetList"])
        tables: dict[str, Table] = {}

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables[cte["ctename"]] = Table(
                    self._analyze_select(cte["ctequery"], layer + 1, name)
                )

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self._analyze_fromclause(fc, tables, layer, name)

        if len(tables.keys()) == 1:
            for refcols in columns.values():
                for rc in refcols:
                    rc.set_table(next(iter(tables)))

        return node.Select(columns, tables, layer, name)

    def _analyze_insert(self, stmt: dict[str, Any], name: str) -> node.Insert:
        tgtcols = self._analyze_restargets(stmt["cols"])
        rel = stmt["relation"]
        tgttbl = {
            rel["alias"]["aliasname"]
            if "alias" in rel.keys()
            else rel["relname"]: Table(rel["relname"])
        }
        subquery = (
            self._analyze_select(stmt["selectStmt"], 1)
            if "selectStmt" in stmt.keys()
            else None
        )
        return node.Insert(tgtcols, tgttbl, subquery, 0, name)

    def _analyze_update(self, stmt: dict[str, Any], name: str) -> node.Update:
        tgtcols = self._analyze_restargets(stmt["targetList"])
        tables: dict[str, Table] = {}
        rel = stmt["relation"]
        tgttbl = {
            rel["alias"]["aliasname"]
            if "alias" in rel.keys()
            else rel["relname"]: Table(rel["relname"])
        }
        if "fromClause" in stmt.keys():
            for fc in stmt["fromClause"]:
                self._analyze_fromclause(fc, tables, 0, name)

        return node.Update(tgtcols, tgttbl, tables, 0, name)
