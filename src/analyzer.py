import builtins
from typing import Any, Tuple

import tqdm
from pglast import ast, parse_sql

import node
from column import Column
from lineage import Lineage
from table import Table


class Analyzer:
    def __init__(self) -> None:
        self.__rawstmts: list[Tuple[str, Any]] = []

    def load(self, sqls: str, name: str) -> None:
        self.__rawstmts.extend(
            (name.lower(), sql.stmt) for sql in parse_sql(sqls.lower())
        )

    def __index(self) -> None:
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

    def analyze(self) -> Lineage:
        self.__index()
        nodes = self.__analyze()
        return Lineage.create(nodes)

    def __analyze(self) -> list[node.Node]:
        nodes: list[node.Node] = []
        for name, rawstmt in tqdm.tqdm(self.__rawstmts):
            match rawstmt:
                case ast.SelectStmt():
                    analyze_stmt = self.__analyze_select
                case ast.InsertStmt():
                    analyze_stmt = self.__analyze_insert
                case ast.UpdateStmt():
                    analyze_stmt = self.__analyze_update
            nodes.append(analyze_stmt(rawstmt(skip_none=True), name=name))
        return nodes

    def __analyze_fromclause(
        self,
        fc: dict[str, Any],
        tables: dict[str, Table],
        name: str = "",
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables[fc["alias"]["aliasname"]] = Table(
                self.__analyze_select(fc["subquery"], name=name),
            )

        elif fc["@"] == "RangeVar":
            tblnm = fc["alias"]["aliasname"] if "alias" in fc.keys() else fc["relname"]
            tables.setdefault(tblnm, Table(fc["relname"]))

        for v in fc.values():
            if isinstance(v, dict):
                self.__analyze_fromclause(v, tables, name)

    def __analyze_whereclause(
        self, wc: dict[str, Any], tables: dict[str, Table], name: str
    ) -> None:
        if "@" not in wc.keys():
            return

        if wc["@"] == "SelectStmt":
            tables.update(self.__analyze_select(wc, name)._flatten().tables)
            return

        for v in wc.values():
            if isinstance(v, dict):
                self.__analyze_whereclause(v, tables, name)

    def __analyze_restargets(
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
                        self.__analyze_restarget(vv, srccols, refcols)
                else:
                    self.__analyze_restarget(v, srccols, refcols)

            name = tgt.get(
                "name", srccols[0].name if len(srccols) == 1 else "column-" + str(i + 1)
            )
            ls = [nm for nm in srcs.keys() if nm.startswith(name)]
            name += "(" + str(len(ls) + 1) + ")" if len(ls) else ""
            srcs[name] = srccols
            refs[name] = refcols

        return srcs, refs

    def __traverse(self, key, tgt, types: list[str]):
        match type(tgt):
            case builtins.dict:
                if tgt.get("@", "") in types:
                    yield key, tgt
                for k, v in tgt.items():
                    for t in list(self.__traverse(k, v, types)):
                        yield t

            case builtins.tuple:
                for v in tgt:
                    if isinstance(v, (tuple, dict)):
                        for t in list(self.__traverse("", v, types)):
                            yield t

    def __extract_caseexpr(self, tgt) -> Tuple[list[Column], list[Column]]:
        srccols: list[Column] = []
        refcols: list[Column] = []

        arg = tgt.get("arg", {})
        if arg.get("@", "") == "ColumnRef":
            col = self.__collect_column(arg)
            if col:
                refcols.append(col)

        TYPES = ["SelectStmt", "ColumnRef"]

        args = tgt.get("args", ())
        for arg in args:
            if arg["@"] == "CaseWhen":
                for next_tgt in self.__traverse("expr", arg["expr"], TYPES):
                    nt = next_tgt[1]
                    match nt.get("@", ""):
                        case "SelectStmt":
                            stmt = self.__analyze_select(nt)._flatten()
                            for sc, rc in zip(
                                stmt.srccols.values(), stmt.refcols.values()
                            ):
                                refcols.extend(sc)
                                refcols.extend(rc)

                        case "ColumnRef":
                            col = self.__collect_column(nt)
                            if col:
                                refcols.append(col)
                for next_tgt in self.__traverse("result", arg["result"], TYPES):
                    nt = next_tgt[1]
                    match nt.get("@", ""):
                        case "SelectStmt":
                            stmt = self.__analyze_select(nt)._flatten()
                            for sc, rc in zip(
                                stmt.srccols.values(), stmt.refcols.values()
                            ):
                                srccols.extend(sc)
                                refcols.extend(rc)
                        case "ColumnRef":
                            col = self.__collect_column(nt)
                            if col:
                                srccols.append(col)

        defresult = tgt.get("defresult", {})
        for next_tgt in self.__traverse("defresult", defresult, TYPES):
            nt = next_tgt[1]
            match nt.get("@", ""):
                case "SelectStmt":
                    stmt = self.__analyze_select(nt)._flatten()
                    for sc, rc in zip(stmt.srccols.values(), stmt.refcols.values()):
                        srccols.extend(sc)
                        refcols.extend(rc)
                case "ColumnRef":
                    col = self.__collect_column(nt)
                    if col:
                        srccols.append(col)

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

    def __collect_column(self, tgt) -> Column | None:
        if "fields" not in tgt:
            return None

        col = []
        for field in tgt["fields"]:
            if "sval" in field.keys():
                col.append(field["sval"])

        return Column.create_from_list(col) if col else None

    def __analyze_restarget(
        self, tgt: dict[str, Any], srccols: list[Column], refcols: list[Column]
    ) -> None:
        if not isinstance(tgt, dict):
            return None

        TYPES = ["ColumnRef", "SelectStmt", "CaseExpr"]
        for rt in self.__traverse("ResTarget", tgt, TYPES):
            t = rt[1]
            match t.get("@", ""):
                case "ColumnRef":
                    col = self.__collect_column(t)
                    if col:
                        srccols.append(col)
                case "SelectStmt":
                    stmt = self.__analyze_select(t)._flatten()
                    for sc in stmt.srccols.values():
                        srccols.extend(sc)
                    for rc in stmt.refcols.values():
                        refcols.extend(rc)
                    return
                case "CaseExpr":
                    res = self.__extract_caseexpr(t)
                    srccols.extend(res[0])
                    refcols.extend(res[1])
                    return

    def __analyze_select(
        self, statement: dict[str, Any], name: str = ""
    ) -> node.Select:
        tables: dict[str, Table] = {}

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables[cte["ctename"]] = Table(
                    self.__analyze_select(cte["ctequery"], name)
                )

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self.__analyze_fromclause(fc, tables, name)

        srccols, refcols = self.__analyze_restargets(statement["targetList"])

        if len(tables.keys()) == 1:
            t = next(iter(tables))
            for scs, rcs in zip(srccols.values(), refcols.values()):
                for sc in scs:
                    sc.set_table(t)
                for rc in rcs:
                    rc.set_table(t)

        if "whereClause" in statement.keys():
            self.__analyze_whereclause(statement["whereClause"], tables, name)

        return node.Select(srccols, refcols, tables, name)

    def __analyze_insert(self, stmt: dict[str, Any], name: str) -> node.Insert:
        srccols, refcols = self.__analyze_restargets(stmt["cols"])
        rel = stmt["relation"]
        tgttbl: dict[str, Table] = {
            rel["alias"]["aliasname"]
            if "alias" in rel.keys()
            else rel["relname"]: Table(rel["relname"])
        }
        subquery = (
            self.__analyze_select(stmt["selectStmt"], 1)
            if "selectStmt" in stmt.keys()
            else None
        )
        return node.Insert(srccols, refcols, tgttbl, subquery, name)

    def __analyze_update(self, stmt: dict[str, Any], name: str) -> node.Update:
        rel = stmt["relation"]
        tgttbl = {
            rel["alias"]["aliasname"]
            if "alias" in rel.keys()
            else rel["relname"]: Table(rel["relname"])
        }

        tables: dict[str, Table] = {}

        if "fromClause" in stmt.keys():
            for fc in stmt["fromClause"]:
                self.__analyze_fromclause(fc, tables, name)

        srccols, refcols = self.__analyze_restargets(stmt["targetList"])

        if not tables:
            for scs, rcs in zip(srccols.values(), refcols.values()):
                for sc in scs:
                    sc.set_table(next(iter(tgttbl)))
                for rc in rcs:
                    rc.set_table(next(iter(tgttbl)))

        if "whereClause" in stmt.keys():
            self.__analyze_whereclause(stmt["whereClause"], tables, 1, name)

        return node.Update(srccols, refcols, tgttbl, tables, name)
