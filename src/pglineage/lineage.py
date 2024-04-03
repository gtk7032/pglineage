from __future__ import annotations

import re
from typing import Tuple

import graphviz as gv
import tqdm
from pglineage import node
from pglineage.edge import ColEdge, TblEdge
from pglineage.logger import Logger
from pglineage.stmt import RawStmt
from pglineage.table import Table

logger = Logger()


class Lineage:
    def __init__(
        self,
        tables: dict[str, Table],
        col_edges: set[ColEdge],
        tbl_edges: set[TblEdge],
        ref_edges: set[TblEdge],
        nodes: list[str],
    ) -> None:
        self.__tables = tables
        self.__col_edges = col_edges
        self.__tbl_edges = tbl_edges
        self.__ref_edges = ref_edges
        self.__nodes = nodes
        self.__dot: gv.Digraph = None
        self.__bar: tqdm.tqdm = None

    @staticmethod
    def __merge(nodes: list[Tuple[RawStmt, node.Node]]) -> Lineage:
        _nodes: list[str] = []
        tgt_tables_insert: dict[str, Table] = {}
        tgt_tables_other: dict[str, Table] = {}
        src_tables: dict[str, Table] = {}
        ref_tables: set[Table] = set()
        col_edges: set[ColEdge] = set()
        tbl_edges: set[TblEdge] = set()
        ref_edges: set[TblEdge] = set()

        for _nd in tqdm.tqdm(nodes, desc="creating", leave=False):
            nm, nd = _nd[0].name, _nd[1]
            try:
                summary: node.Summary = nd.summary(nm)
            except Exception as e:
                logger.set("failed", str(e), _nd[0])
                continue

            if (
                not summary.src_tbls
                and not summary.ref_tbls
                and isinstance(nd, node.Select)
            ):
                continue

            _nodes.append(nm)

            nm = next(iter(summary.tgt_tbl.keys()))
            tgt_tables = (
                tgt_tables_insert if isinstance(nd, node.Insert) else tgt_tables_other
            )
            tgt_tables.setdefault(nm, Table(nm))
            tgt_tables[nm].update(summary.tgt_tbl[nm].columns)

            for nm, tbl in summary.src_tbls.items():
                src_tables.setdefault(nm, Table(nm))
                src_tables[nm].update(tbl.columns)

            ref_tables.update(summary.ref_tbls)
            col_edges.update(summary.col_edges)
            tbl_edges.update(summary.tbl_edges)
            ref_edges.update(summary.ref_edges)

        tables: dict[str, Table] = {}
        tables = tgt_tables_other | tgt_tables_insert

        for k, v in src_tables.items():
            if k in tables.keys():
                tables[k].update(v.columns)
            else:
                tables[k] = v

        for t in ref_tables:
            tables.setdefault(t, Table(t))

        ref_edges = {e for e in ref_edges if e not in tbl_edges}

        return Lineage(
            tables,
            col_edges,
            tbl_edges,
            ref_edges,
            _nodes,
        )

    @staticmethod
    def create(nodes: list[Tuple[RawStmt, node.Node]]) -> Lineage:
        return Lineage.__merge(nodes)

    def __bundled(self) -> Lineage:
        ptrn = re.compile("-[0-9]+$")

        def f1(nd: str) -> str:
            return nd if nd.startswith(node.Select.STATEMENT) else re.sub(ptrn, "", nd)

        def f2(edge: TblEdge) -> TblEdge:
            return ColEdge(f1(edge.tail), f1(edge.head))

        nds = {f1(nd) for nd in self.__nodes}
        tes = {f2(te) for te in self.__tbl_edges}
        res = {f2(_re) for _re in self.__ref_edges} - tes
        return Lineage(self.__tables, self.__col_edges, tes, res, nds)

    def draw(self, output: str = "output/result", format: str = "png") -> None:
        self.__bar = tqdm.tqdm(total=3, desc="drawing", leave=False)
        self.__draw_column_level(output + ".clv", format)
        self.__bar.update(1)
        self.__bundled().__draw_table_level(output + "_bundled_" + ".tlv", format)
        self.__bar.update(1)
        self.__draw_table_level(output + ".tlv", format)
        self.__bar.update(1)
        logger.write(output + ".log")

    def __draw_column_level(self, output: str, format: str) -> None:
        self.__dot = gv.Digraph(format=format)
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")

        used_tables = set()
        for e in self.__col_edges:
            used_tables.add(e.head.table)
            used_tables.add(e.tail.table)

        for tbl in self.__tables.values():
            if tbl.name not in used_tables:
                continue
            if not tbl.columns:
                continue
            xlabel = self.__out_table(tbl.name)
            label = ""
            for col in tbl.columns:
                sep = "|" if label else ""
                label += sep + "<" + col + "> " + col
            self.__dot.node(tbl.name, shape="record", label=label, xlabel=xlabel)

        for edge in self.__col_edges:
            self.__dot.edge(
                edge.tail.table + ":" + edge.tail.name,
                edge.head.table + ":" + edge.head.name,
            )

        self.__dot.render(output)

    def __draw_table_level(self, output: str, format: str) -> None:
        self.__dot = gv.Digraph(format=format)
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")

        for tbl in self.__tables.values():
            self.__dot.node(
                tbl.name,
                shape="cylinder",
                label=self.__out_table(tbl.name),
            )

        for name in self.__nodes:
            self.__dot.node(name, label=self.__out_table(name), shape="note")

        for edge in self.__tbl_edges:
            self.__dot.edge(edge.tail, edge.head)

        for edge in self.__ref_edges:
            self.__dot.edge(edge.tail, edge.head, style="dashed")

        self.__dot.render(output)

    def __out_table(self, tbl: str) -> str:
        return "" if tbl.startswith(node.Select.STATEMENT) else tbl
