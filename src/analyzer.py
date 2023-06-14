import builtins
from typing import Any, Tuple

import tqdm
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
        for name, rawstmt in tqdm.tqdm(self.__rawstmts):
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

    def _analyze_whereclause(
        self, wc: dict[str, Any], tables: dict[str, Table], layer: int, name: str
    ) -> None:
        if "@" not in wc.keys():
            return

        if wc["@"] == "SelectStmt":
            tables.update(self._analyze_select(wc, layer + 1, name)._flatten().tables)
            return

        for v in wc.values():
            if isinstance(v, dict):
                self._analyze_whereclause(v, tables, layer + 1, name)

    def _analyze_case(self, tgt) -> None:
        if tgt.get("@", "") != "CaseExpr":
            Exception()

    def _analyze_restargets(
        self, restargets: list[dict[str, Any]]
    ) -> Tuple[dict[str, list[Column]], dict[str, list[Column]]]:
        srcs: dict[str, list[Column]] = {}
        refs: dict[str, list[Column]] = {}

        for i, tgt in enumerate(restargets):
            if tgt.get("@", "") != "ResTarget":
                Exception()

            srccols: list[Column] = []
            refcols: list[Column] = []

            for v in tgt.values():
                if isinstance(v, tuple):
                    for vv in v:
                        self._analyze_restarget(vv, srccols, refcols)
                else:
                    self._analyze_restarget(v, srccols, refcols)

            name = tgt.get(
                "name", srccols[0].name if len(srccols) == 1 else "column-" + str(i + 1)
            )
            ls = [nm for nm in srcs.keys() if nm.startswith(name)]
            name += "(" + str(len(ls) + 1) + ")" if len(ls) else ""
            srcs[name] = srccols
            refs[name] = refcols

        return srcs, refs

    def traverse(self, key, tgt, types: list[str]):
        match type(tgt):
            case builtins.dict:
                if tgt.get("@", "") in types:
                    yield key, tgt
                for k, v in tgt.items():
                    for t in list(self.traverse(k, v, types)):
                        yield t

            case builtins.tuple:
                for v in tgt:
                    if isinstance(v, (tuple, dict)):
                        for t in list(self.traverse("", v, types)):
                            yield t

    def _extract_caseexpr(self, tgt) -> Tuple[list[Column], list[Column]]:
        srccols: list[Column] = []
        refcols: list[Column] = []

        arg = tgt.get("arg", {})
        if arg.get("@", "") == "ColumnRef":
            col = self._collect_column(arg)
            if col:
                refcols.append(col)

        def func(tgt, srccols=None, refcols=None):
            match tgt.get("@", ""):
                case "SelectStmt":
                    stmt = self._analyze_select(tgt)._flatten()
                    if srccols is not None:
                        for sc in stmt.srccols.values():
                            srccols.extend(sc)
                    if refcols is not None:
                        for rc in stmt.refcols.values():
                            print(str(rc))
                            refcols.extend(rc)
                case "ColumnRef":
                    col = self._collect_column(tgt)
                    if col:
                        if srccols is not None:
                            srccols.append(col)
                        if refcols is not None:
                            refcols.append(col)

        types = ["SelectStmt", "ColumnRef"]

        args = tgt.get("args", ())
        for arg in args:
            if arg["@"] == "CaseWhen":
                for next_tgt in self.traverse("expr", arg["expr"], types):
                    func(next_tgt[1], None, refcols)
                for next_tgt in self.traverse("result", arg["result"], types):
                    func(next_tgt[1], srccols, refcols)

        defresult = tgt.get("defresult", {})
        for next_tgt in self.traverse("defresult", defresult, types):
            func(next_tgt[1], srccols, refcols)

        def uniq(cols):
            u = set()
            res = []
            for c in cols:
                s = str(c)
                if s not in u:
                    u.add(s)
                    res.append(c)
            return res

        return uniq(srccols), uniq(refcols)

    def _collect_column(self, tgt) -> Column | None:
        if "fields" not in tgt:
            return None

        col = []
        for field in tgt["fields"]:
            if "sval" in field.keys():
                col.append(field["sval"])

        return Column.create_from_list(col) if col else None

    def _analyze_restarget(
        self, tgt: dict[str, Any], srccols: list[Column], refcols: list[Column]
    ) -> None:
        if not isinstance(tgt, dict):
            return None

        TYPES = ["ColumnRef", "SelectStmt", "CaseExpr"]
        for rt in self.traverse("ResTarget", tgt, TYPES):
            t = rt[1]
            match t.get("@", ""):
                case "ColumnRef":
                    col = self._collect_column(t)
                    if col:
                        srccols.append(col)
                case "SelectStmt":
                    stmt = self._analyze_select(t)._flatten()
                    for sc in stmt.srccols.values():
                        srccols.extend(sc)
                    for rc in stmt.refcols.values():
                        refcols.extend(rc)
                case "CaseExpr":
                    srccols, refcols = self._extract_caseexpr(t)

        return None

    def _analyze_select(
        self, statement: dict[str, Any], layer: int = 0, name: str = ""
    ) -> node.Select:
        srccols, refcols = self._analyze_restargets(statement["targetList"])
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
            for scs, rcs in zip(srccols.values(), refcols.values()):
                for sc in scs:
                    sc.set_table(next(iter(tables)))
                for rc in rcs:
                    rc.set_table(next(iter(tables)))

        if "whereClause" in statement.keys():
            self._analyze_whereclause(statement["whereClause"], tables, layer + 1, name)

        return node.Select(srccols, refcols, tables, layer, name)

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
        rel = stmt["relation"]
        tgttbl = {
            rel["alias"]["aliasname"]
            if "alias" in rel.keys()
            else rel["relname"]: Table(rel["relname"])
        }

        tables: dict[str, Table] = {}

        if "fromClause" in stmt.keys():
            for fc in stmt["fromClause"]:
                self._analyze_fromclause(fc, tables, 0, name)

        tgtcols = self._analyze_restargets(stmt["targetList"])

        if len(tables.keys()) == 1:
            for refcols in tgtcols.values():
                if isinstance(refcols, list):
                    for rc in refcols:
                        rc.set_table(next(iter(tables)))

        for refcols in tgtcols.values():
            if isinstance(refcols, node.Select):
                f = refcols._flatten()
                tables.update(f.tables)
                refcols = next(iter(f.srccols.values()))
            elif isinstance(refcols, list):
                for refcol in refcols:
                    if refcol.use == 1 and refcol.table:
                        tables.setdefault(refcol.table, Table(refcol.table))

        for tgtcol, refcols in tgtcols.items():
            tgtcols[tgtcol] = [refcol for refcol in refcols if refcol.use == 0]

        if "whereClause" in stmt.keys():
            self._analyze_whereclause(stmt["whereClause"], tables, 1, name)

        return node.Update(tgtcols, tgttbl, tables, 0, name)
