import builtins
from typing import Any, Tuple

import tqdm
from pglast import ast, parse_sql

import node
from column import Column
from lineage import Lineage


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

    def __analyze(self) -> list[Tuple[str, node.Node]]:
        nodes: list[Tuple[str, node.Node]] = []
        for name, rawstmt in tqdm.tqdm(self.__rawstmts, desc="analyzing", leave=False):
            match rawstmt:
                case ast.SelectStmt():
                    analyze_stmt = self.__analyze_select
                case ast.InsertStmt():
                    analyze_stmt = self.__analyze_insert
                case ast.UpdateStmt():
                    analyze_stmt = self.__analyze_update
            nodes.append((name, analyze_stmt(rawstmt(skip_none=True))))
        return nodes

    def __analyze_fromclause(
        self, fc: dict[str, Any], tables: dict[str, str | node.Select]
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables[fc["alias"]["aliasname"]] = self.__analyze_select(fc["subquery"])

        elif fc["@"] == "RangeVar":
            tblnm = fc["alias"]["aliasname"] if "alias" in fc.keys() else fc["relname"]
            tables.setdefault(tblnm, fc["relname"])

        for v in fc.values():
            if isinstance(v, dict):
                self.__analyze_fromclause(v, tables)

    def __analyze_whereclause(
        self, wc: dict[str, Any], tables: dict[str, str | node.Select]
    ) -> None:
        if "@" not in wc.keys():
            return

        if wc["@"] == "SelectStmt":
            tables.update(self.__analyze_select(wc)._flatten().tables)
            return

        for v in wc.values():
            if isinstance(v, dict):
                self.__analyze_whereclause(v, tables)

    def __analyze_restargets(
        self, restargets: list[dict[str, Any]]
    ) -> Tuple[
        dict[str, list[Column]], dict[str, list[Column]], dict[str, str | node.Select]
    ]:
        srcs: dict[str, list[Column]] = {}
        refs: dict[str, list[Column]] = {}
        tables: dict[str, str | node.Select] = {}

        for i, tgt in enumerate(restargets):
            if tgt.get("@", "") != "ResTarget":
                Exception()

            srccols: list[Column] = []
            refcols: list[Column] = []

            for v in tgt.values():
                if isinstance(v, tuple):
                    for vv in v:
                        self.__analyze_restarget(vv, srccols, refcols, tables)
                else:
                    self.__analyze_restarget(v, srccols, refcols, tables)

            name = tgt.get(
                "name", srccols[0].name if len(srccols) == 1 else "column-" + str(i + 1)
            )
            ls = [nm for nm in srcs.keys() if nm.startswith(name)]
            name += "(" + str(len(ls) + 1) + ")" if len(ls) else ""
            srcs[name] = srccols
            refs[name] = refcols

        return srcs, refs, tables

    def __analyze_valueslist(
        self, valuelist: Tuple[dict[str, Any]]
    ) -> Tuple[
        dict[str, list[Column]], dict[str, list[Column]], dict[str, str | node.Select]
    ]:
        srccols: dict[str, list[Column]] = {}
        refcols: dict[str, list[Column]] = {}
        tables: dict[str, str | node.Select] = {}

        for i, v in enumerate(valuelist):
            if not isinstance(v, dict):
                raise Exception()
            if "@" not in v.keys():
                raise Exception()

            scs: list[Column] = []
            rcs: list[Column] = []
            tbls: list[str] = []
            match v["@"]:
                case "CaseExpr":
                    scs, rcs, _tbls = self.__extract_caseexpr(v)
                    tbls = list(_tbls.values())
                case "SelectStmt":
                    res = self.__analyze_select(v)._flatten()
                    scs, rcs, tbls = (
                        next(iter(res.srccols.values())),
                        next(iter(res.refcols.values())),
                        list(res.tables.values()),
                    )
                case _:
                    pass
            srccols["column-" + str(i + 1)] = scs
            refcols["column-" + str(i + 1)] = rcs
            tables.update({t: t for t in tbls})

        return srccols, refcols, tables

    def __analyze_valueslists(
        self, valueslists: Tuple[Tuple[dict[str, Any]]]
    ) -> Tuple[
        dict[str, list[Column]], dict[str, list[Column]], dict[str, str | node.Select]
    ]:
        srccols: dict[str, list[Column]] = {}
        refcols: dict[str, list[Column]] = {}
        tables: dict[str, str | node.Select] = {}

        for vl in valueslists:
            scs, rcs, tbls = self.__analyze_valueslist(vl)
            tables.update(tbls)
            srccols.update(scs)
            refcols.update(rcs)

        return srccols, refcols, tables

    def __traverse(self, key, tgt, types: list[str]):
        match type(tgt):
            case builtins.dict:
                if tgt.get("@", "") in types:
                    yield key, tgt
                    return
                for k, v in tgt.items():
                    for t in list(self.__traverse(k, v, types)):
                        yield t
            case builtins.tuple:
                for v in tgt:
                    if isinstance(v, (tuple, dict)):
                        for t in list(self.__traverse("", v, types)):
                            yield t

    def __extract_caseexpr(
        self, tgt
    ) -> Tuple[list[Column], list[Column], dict[str, str | node.Select]]:
        srccols: list[Column] = []
        refcols: list[Column] = []
        tables: dict[str, str | node.Select] = {}

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
                            tables.update(stmt.tables)

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
                            tables.update(stmt.tables)

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
                    tables.update(stmt.tables)

                case "ColumnRef":
                    col = self.__collect_column(nt)
                    if col:
                        srccols.append(col)

        return list(set(srccols)), list(set(refcols)), tables

    def __collect_column(self, tgt) -> Column | None:
        if "fields" not in tgt:
            return None

        col = []
        for field in tgt["fields"]:
            if "sval" in field.keys():
                col.append(field["sval"])

        return Column.create_from_list(col) if col else None

    def __analyze_restarget(
        self,
        tgt: dict[str, Any],
        srccols: list[Column],
        refcols: list[Column],
        tables: dict[str, str | node.Select],
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
                    tables.update(stmt.tables)
                    return

                case "CaseExpr":
                    res = self.__extract_caseexpr(t)
                    srccols.extend(res[0])
                    refcols.extend(res[1])
                    tables.update(res[2])
                    return

    def __analyze_select(self, statement: dict[str, Any]) -> node.Select:
        tables: dict[str, str | node.Select] = {}

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables[cte["ctename"]] = self.__analyze_select(cte["ctequery"])

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self.__analyze_fromclause(fc, tables)

        if "targetList" in statement.keys():
            srccols, refcols, _tbls = self.__analyze_restargets(statement["targetList"])
        elif "valuesLists" in statement.keys():
            srccols, refcols, _tbls = self.__analyze_valueslists(
                statement["valuesLists"]
            )

        if len(tables.keys()) == 1:
            t = next(iter(tables))
            for scs, rcs in zip(srccols.values(), refcols.values()):
                for sc in scs:
                    sc.set_table(t)
                for rc in rcs:
                    rc.set_table(t)

        if "whereClause" in statement.keys():
            self.__analyze_whereclause(statement["whereClause"], tables)

        return node.Select(srccols, refcols, tables | _tbls)

    def __analyze_insert(self, stmt: dict[str, Any]) -> node.Insert:
        tgttable = stmt["relation"]["relname"]

        tgtcols, _, _ = self.__analyze_restargets(stmt["cols"])
        for scs in tgtcols.values():
            for sc in scs:
                sc.set_table(tgttable)

        srccols: dict[str, list[Column]] = {}
        refcols: dict[str, list[Column]] = {}

        subquery = self.__analyze_select(stmt["selectStmt"])

        for tgtcol, _srccols, _refcols in zip(
            tgtcols, subquery.srccols.values(), subquery.refcols.values()
        ):
            srccols[tgtcol] = _srccols
            refcols[tgtcol] = _refcols

        return node.Insert(srccols, refcols, tgttable, subquery.tables)

    def __analyze_update(self, stmt: dict[str, Any]) -> node.Update:
        rel = stmt["relation"]
        tgttbl = {
            "alias": rel["alias"]["aliasname"] if "alias" in rel.keys() else "",
            "name": rel["relname"],
        }

        tables: dict[str, str | node.Select] = {}

        if "fromClause" in stmt.keys():
            for fc in stmt["fromClause"]:
                self.__analyze_fromclause(fc, tables)

        srccols, refcols, _tbls = self.__analyze_restargets(stmt["targetList"])

        if not tables:
            for scs, rcs in zip(srccols.values(), refcols.values()):
                for sc in scs:
                    sc.replace_table(tgttbl["alias"], tgttbl["name"])
                for rc in rcs:
                    rc.replace_table(tgttbl["alias"], tgttbl["name"])

        if "whereClause" in stmt.keys():
            self.__analyze_whereclause(stmt["whereClause"], tables)

        return node.Update(
            srccols, refcols, {tgttbl["alias"]: tgttbl["name"]}, tables | _tbls
        )
