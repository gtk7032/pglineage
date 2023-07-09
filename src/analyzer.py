import builtins
from typing import Any, Tuple

import tqdm
from pglast import ast, parse_sql

import node
from column import Column
from lineage import Lineage
from logger import Logger, Row

logger = Logger()


class Analyzer:
    def __init__(self) -> None:
        self.__stmts: list[dict[str, str]] = []

    def load(self, sqls: list[Tuple[str, str]]) -> None:
        for name, sql in tqdm.tqdm(sqls, desc="loading", leave=False):
            self.__stmts.append({"name": name, "rawstmt": sql})
            logger.set(name, Row(name, "success", "", sql))

    def __parse(self) -> None:
        for stmt in tqdm.tqdm(self.__stmts, desc="parsing", leave=False):
            try:
                stmt["psdstmt"] = next(iter(parse_sql(stmt["rawstmt"]))).stmt
            except Exception as e:
                # import traceback
                # print(traceback.format_exc())
                stmt["psdstmt"] = ""
                logger.set(stmt["name"], Row(stmt["name"], "failed", str(e), stmt["rawstmt"]))

                continue

    def analyze(self) -> Lineage:
        self.__parse()
        return Lineage.create(self.__analyze())

    def __analyze(self) -> list[Tuple[str, str, node.Node]]:
        nodes: list[Tuple[str, str, node.Node]] = []
        for stmt in tqdm.tqdm(self.__stmts, desc="analyzing", leave=False):
            match stmt["psdstmt"]:
                case ast.SelectStmt():
                    analyze_stmt = self.__analyze_select
                case ast.InsertStmt():
                    analyze_stmt = self.__analyze_insert
                case ast.UpdateStmt():
                    analyze_stmt = self.__analyze_update
                case ast.DeleteStmt():
                    analyze_stmt = self.__analyze_delete
                case _:
                    continue
            try:
                # from pprint import pprint
                # pprint(stmt["psdstmt"](skip_none=True))
                nd = (
                    stmt["name"],
                    stmt["rawstmt"],
                    analyze_stmt(stmt["psdstmt"](skip_none=True)),
                )
                nodes.append(nd)

            except Exception as e:
                logger.set(stmt["name"], Row(stmt["name"], "failed", str(e), stmt["rawstmt"]))
                # import traceback
                # print(traceback.format_exc())
                continue

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

    def __merge_tables(
        self, fst: dict[str, str | node.Select], snd: dict[str, str | node.Select]
    ) -> None:
        for sk, sv in snd.items():
            if isinstance(sv, node.Select):
                fst[sk] = sv
            elif isinstance(sv, str):
                fst.setdefault(sk, sv)

    def __analyze_whereclause(
        self, wc: dict[str, Any], tables: dict[str, str | node.Select]
    ) -> None:
        if "@" not in wc.keys():
            return

        if wc["@"] == "SelectStmt":
            _tbls = self.__analyze_select(wc)._flatten().tables
            self.__merge_tables(tables, _tbls)
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
            self.__merge_tables(tables, {t: t for t in tbls})

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
            self.__merge_tables(tables, tbls)
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
                            self.__merge_tables(tables, stmt.tables)

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
                            self.__merge_tables(tables, stmt.tables)

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
                    self.__merge_tables(tables, stmt.tables)

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

    def __analyze_multiassignref(
        self, tgt: dict[str, Any]
    ) -> Tuple[
        dict[str, list[Column]], dict[str, list[Column]], dict[str, str | node.Select]
    ]:
        srccols: dict[str, list[Column]] = {}
        refcols: dict[str, list[Column]] = {}

        colno = int(tgt["colno"])
        subselect = next(iter(self.__traverse("source", tgt["source"], ["SelectStmt"])))
        subselect = self.__analyze_select(subselect[1])._flatten()
        k = "column-" + str(colno)
        for i, (scs, rcs) in enumerate(
            zip(subselect.srccols.values(), subselect.refcols.values())
        ):
            if i + 1 == colno:
                srccols[k] = scs
                refcols[k] = rcs
                break

        return srccols, refcols, subselect.tables

    def __analyze_restarget(
        self,
        tgt: dict[str, Any],
        srccols: list[Column],
        refcols: list[Column],
        tables: dict[str, str | node.Select],
    ) -> None:
        if not isinstance(tgt, dict):
            return None

        TYPES = ["ColumnRef", "SelectStmt", "CaseExpr", "MultiAssignRef"]
        for rt in self.__traverse("ResTarget", tgt, TYPES):
            t = rt[1]
            match t.get("@", ""):
                case "MultiAssignRef":
                    scs, rcs, tbls = self.__analyze_multiassignref(t)
                    srccols.extend(next(iter(scs.values())))
                    refcols.extend(next(iter(rcs.values())))
                    self.__merge_tables(tables, tbls)
                    return

                case "ColumnRef":
                    col = self.__collect_column(t)
                    if col:
                        srccols.append(col)
                    return

                case "SelectStmt":
                    stmt = self.__analyze_select(t)._flatten()
                    for sc in stmt.srccols.values():
                        srccols.extend(sc)
                    for rc in stmt.refcols.values():
                        refcols.extend(rc)
                    self.__merge_tables(tables, stmt.tables)
                    return

                case "CaseExpr":
                    res = self.__extract_caseexpr(t)
                    srccols.extend(res[0])
                    refcols.extend(res[1])
                    self.__merge_tables(tables, res[2])
                    return

    def __analyze_withclause(self, wc: dict[str, Any]) -> node.Select:
        cte = self.__analyze_select(wc["ctequery"])
        if "aliascolnames" in wc.keys():
            cte.srccols = {
                colalias["sval"]: srccols
                for colalias, srccols in zip(wc["aliascolnames"], cte.srccols.values())
            }
            cte.refcols = {
                colalias["sval"]: refcols
                for colalias, refcols in zip(wc["aliascolnames"], cte.refcols.values())
            }
        return cte

    def __analyze_select(self, statement: dict[str, Any]) -> node.Select:
        tables: dict[str, str | node.Select] = {}

        if "withClause" in statement.keys():
            tbls = {}
            for cte in statement["withClause"]["ctes"]:
                tbls[cte["ctename"]] = self.__analyze_withclause(cte)
            self.__merge_tables(tables, tbls)

        if "op" in statement.keys():
            if statement["op"]["name"] == "SETOP_UNION":
                left = self.__analyze_select(statement["larg"])
                right = self.__analyze_select(statement["rarg"])
                scs = {
                    lk if lk == rk else "column-" + str(i + 1): list(set(lv + rv))
                    for i, ((lk, lv), (rk, rv)) in enumerate(
                        zip(left.srccols.items(), right.srccols.items())
                    )
                }
                rcs = {
                    lk if lk == rk else "column-" + str(i + 1): list(set(lv + rv))
                    for i, ((lk, lv), (rk, rv)) in enumerate(
                        zip(left.refcols.items(), right.refcols.items())
                    )
                }
                self.__merge_tables(left.tables, right.tables)
                return node.Select(scs, rcs, left.tables)

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self.__analyze_fromclause(fc, tables)

        srccols, refcols, _tbls = {}, {}, {}
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

        self.__merge_tables(tables, _tbls)

        if "whereClause" in statement.keys():
            self.__analyze_whereclause(statement["whereClause"], tables)

        return node.Select(srccols, refcols, tables)

    def __analyze_insert(self, stmt: dict[str, Any]) -> node.Insert:
        tgttable = stmt["relation"]["relname"]

        tgtcols, _, _ = self.__analyze_restargets(stmt["cols"])
        for scs in tgtcols.values():
            for sc in scs:
                sc.set_table(tgttable)

        srccols: dict[str, list[Column]] = {}
        refcols: dict[str, list[Column]] = {}

        tables: dict[str, str | node.Select] = {}
        subquery = self.__analyze_select(stmt["selectStmt"])
        self.__merge_tables(tables, subquery.tables)

        for tgtcol, _srccols, _refcols in zip(
            tgtcols, subquery.srccols.values(), subquery.refcols.values()
        ):
            srccols[tgtcol] = _srccols
            refcols[tgtcol] = _refcols

        if "withClause" in stmt.keys():
            tbls = {}
            for cte in stmt["withClause"]["ctes"]:
                tbls[cte["ctename"]] = self.__analyze_withclause(cte)
            self.__merge_tables(tables, tbls)

        return node.Insert(srccols, refcols, tgttable, tables)

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

        self.__merge_tables(tables, _tbls)

        if "withClause" in stmt.keys():
            tbls = {}
            for cte in stmt["withClause"]["ctes"]:
                tbls[cte["ctename"]] = self.__analyze_withclause(cte)
            self.__merge_tables(tables, tbls)

        if "whereClause" in stmt.keys():
            self.__analyze_whereclause(stmt["whereClause"], tables)

        return node.Update(srccols, refcols, {tgttbl["alias"]: tgttbl["name"]}, tables)

    def __analyze_delete(self, stmt: dict[str, Any]) -> node.Delete:
        rel = stmt["relation"]
        tgttbl = {
            "alias": rel["alias"]["aliasname"] if "alias" in rel.keys() else "",
            "name": rel["relname"],
        }

        tables: dict[str, str | node.Select] = {}

        if "whereClause" in stmt.keys():
            self.__analyze_whereclause(stmt["whereClause"], tables)

        if "withClause" in stmt.keys():
            tbls = {}
            for cte in stmt["withClause"]["ctes"]:
                tbls[cte["ctename"]] = self.__analyze_withclause(cte)
            self.__merge_tables(tables, tbls)

        return node.Delete({tgttbl["alias"]: tgttbl["name"]}, tables)
