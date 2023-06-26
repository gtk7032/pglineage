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
    ) -> None:
        self.__src_tbls: dict[str, Table] = {}
        self.__tgt_tbls: dict[str, Table] = {}
        self.__ref_tbls: set[str] = set()
        self.__col_edges: set[ColEdge] = set()
        self.__tbl_edges: set[TblEdge] = set()
        self.__ref_edges: set[TblEdge] = set()
        self.__dot: gv.Digraph = None
        self.__nodes: list[str] = []
        self.__bar: tqdm.tqdm = None

    @staticmethod
    def merge(nodes: list[Tuple[str, node.Node]]) -> Lineage:
        lineage = Lineage()

        for _nd in tqdm.tqdm(nodes, desc="creating", leave=False):
            nm, nd = _nd[0], _nd[1]
            lineage.__nodes.append(nm)

            summary: node.Summary = nd.summary(nm)

            if (
                not summary.src_tbls
                and not summary.ref_tbls
                and isinstance(nd, node.Select)
            ):
                continue

            nm = next(iter(summary.tgt_tbl.keys()))
            lineage.__tgt_tbls.setdefault(nm, Table(nm))
            lineage.__tgt_tbls[nm].update(summary.tgt_tbl[nm].columns)

            for nm, tbl in summary.src_tbls.items():
                lineage.__src_tbls.setdefault(nm, Table(nm))
                lineage.__src_tbls[nm].update(tbl.columns)

            lineage.__ref_tbls.update(summary.ref_tbls)

            lineage.__col_edges.update(summary.col_edges)
            lineage.__tbl_edges.update(summary.tbl_edges)
            lineage.__ref_edges.update(summary.ref_edges)

        return lineage

    @staticmethod
    def __sort(nodes: list[Tuple[str, node.Node]]) -> list[Tuple[str, node.Node]]:
        fst, snd = [], []
        for nd in nodes:
            if isinstance(nd[1], node.Insert):
                fst.append(nd)
            else:
                snd.append(nd)
        return fst + snd

    @staticmethod
    def create(nodes: list[Tuple[str, node.Node]]) -> Lineage:
        return Lineage.merge(Lineage.__sort(nodes))

    def draw(self, type: int) -> None:
        self.__dot = gv.Digraph(format="png", filename="pglineage.gv")
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")
        self.__bar = None

        match type:
            case 1:
                self.__draw_1()
            case 2:
                self.__draw_2()
            case 3:
                self.__draw_3()

        self.__dot.render("pglineage")

    def __out_table(self, tbl: str) -> str:
        return "" if tbl.startswith(node.Select.STATEMENT) else tbl

    def __draw_tables(self, tables: list[Table], type: int) -> None:
        if type == 1:
            for tbl in tables:
                xlabel = self.__out_table(tbl.name)
                label = ""
                for col in tbl.columns:
                    sep = "|" if label else ""
                    label += sep + "<" + col + "> " + col
                self.__dot.node(tbl.name, shape="record", label=label, xlabel=xlabel)

        elif type in (2, 3):
            for tbl in tables:
                self.__dot.node(
                    tbl.name, shape="cylinder", label=self.__out_table(tbl.name)
                )

        self.__bar.update(1)

    def _draw_tgttables(self, type: int) -> None:
        self.__draw_tables(list(self.__tgt_tbls.values()), type)

    def _draw_srctables(self, type: int) -> None:
        tmp = [t for t in self.__src_tbls.values() if t.name not in self.__tgt_tbls]
        self.__draw_tables(tmp, type)

    def _draw_reftables(self) -> None:
        for rt in self.__ref_tbls:
            out = self.__out_table(rt)
            if rt not in self.__tgt_tbls and rt not in self.__src_tbls:
                self.__dot.node(rt, shape="cylinder", label=out)
        self.__bar.update(1)

    def _draw_coledges(self) -> None:
        for edge in self.__col_edges:
            self.__dot.edge(
                edge.tail.table + ":" + edge.tail.name,
                edge.head.table + ":" + edge.head.name,
                label="",
            )
        self.__bar.update(1)

    def _draw_tbledges(self) -> None:
        for edge in self.__tbl_edges:
            self.__dot.edge(edge.tail, edge.head)
        self.__bar.update(1)

    def _draw_refedges(self) -> None:
        for edge in self.__ref_edges:
            if edge not in self.__tbl_edges:
                self.__dot.edge(edge.tail, edge.head, style="dashed")
        self.__bar.update(1)

    def _draw_nodes(self) -> None:
        for name in self.__nodes:
            self.__dot.node(name, label=name, shape="note")
        self.__bar.update(1)

    def __draw_1(self) -> None:
        self.__bar = tqdm.tqdm(total=3, desc="drawing", leave=False)
        self._draw_tgttables(1)
        self._draw_srctables(1)
        self._draw_coledges()

    def __draw_2(self) -> None:
        self.__bar = tqdm.tqdm(total=4, desc="drawing", leave=False)
        self._draw_tgttables(2)
        self._draw_srctables(2)
        self._draw_nodes()
        self._draw_tbledges()

    def __draw_3(self) -> None:
        self.__bar = tqdm.tqdm(total=6, desc="drawing", leave=False)
        self._draw_tgttables(3)
        self._draw_srctables(3)
        self._draw_reftables()
        self._draw_nodes()
        self._draw_tbledges()
        self._draw_refedges()
