from __future__ import annotations

from typing import Tuple

import graphviz as gv
import tqdm

import node
from edge import ColEdge, TblEdge
from table import Table


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
    def merge(nodes: list[Tuple[str, node.Node]]) -> Lineage:
        __nodes: list[str] = []
        __tgt_tables_insert: dict[str, Table] = {}
        __tgt_tables_other: dict[str, Table] = {}
        __src_tables: dict[str, Table] = {}
        __ref_tables: set[Table] = set()
        __col_edges: set[ColEdge] = set()
        __tbl_edges: set[TblEdge] = set()
        __ref_edges: set[TblEdge] = set()

        for nd in tqdm.tqdm(nodes, desc="creating", leave=False):
            nm, nd = nd[0], nd[1]
            __nodes.append(nm)

            summary: node.Summary = nd.summary(nm)

            if (
                not summary.src_tbls
                and not summary.ref_tbls
                and isinstance(nd, node.Select)
            ):
                continue

            nm = next(iter(summary.tgt_tbl.keys()))
            __tgt_tables = (
                __tgt_tables_insert
                if isinstance(nd, node.Insert)
                else __tgt_tables_other
            )
            __tgt_tables.setdefault(nm, Table(nm))
            __tgt_tables[nm].update(summary.tgt_tbl[nm].columns)

            for nm, tbl in summary.src_tbls.items():
                __src_tables.setdefault(nm, Table(nm))
                __src_tables[nm].update(tbl.columns)

            __ref_tables.update(summary.ref_tbls)
            __col_edges.update(summary.col_edges)
            __tbl_edges.update(summary.tbl_edges)
            __ref_edges.update(summary.ref_edges)

        __tables: dict[str, Table] = {}
        __tables = __tgt_tables_other | __tgt_tables_insert

        for k, v in __src_tables.items():
            if k in __tgt_tables_insert.keys():
                continue
            if k in __tables.keys():
                __tables[k].update(v.columns)
            else:
                __tables[k] = v

        for t in __ref_tables:
            __tables.setdefault(t, Table(t))

        __ref_edges = {e for e in __ref_edges if e not in __tbl_edges}

        return Lineage(
            __tables,
            __col_edges,
            __tbl_edges,
            __ref_edges,
            __nodes,
        )

    @staticmethod
    def create(nodes: list[Tuple[str, node.Node]]) -> Lineage:
        return Lineage.merge(nodes)

    def draw(self) -> None:
        self.__bar = tqdm.tqdm(total=2, desc="drawing", leave=False)
        self.draw_column_level()
        self.__bar.update(1)
        self.draw_table_level()
        self.__bar.update(1)

    def draw_column_level(self) -> None:
        self.__dot = gv.Digraph(format="png")
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")

        for tbl in self.__tables.values():
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

        self.__dot.render("pglineage-column-level")

    def draw_table_level(self) -> None:
        self.__dot = gv.Digraph(format="png")
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")

        for tbl in self.__tables.values():
            self.__dot.node(
                tbl.name, shape="cylinder", label=self.__out_table(tbl.name)
            )

        for name in self.__nodes:
            self.__dot.node(name, label=name, shape="note")

        for edge in self.__tbl_edges:
            self.__dot.edge(edge.tail, edge.head)

        for edge in self.__ref_edges:
            self.__dot.edge(edge.tail, edge.head, style="dashed")

        self.__dot.render("pglineage-table-level")

    def __out_table(self, tbl: str) -> str:
        return "" if tbl.startswith(node.Select.STATEMENT) else tbl
