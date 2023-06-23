from __future__ import annotations

from typing import Tuple

import graphviz as gv

import node
from column import Column


class Lineage:
    def __init__(
        self,
    ) -> None:
        self._src_tbls: dict[str, dict[str, None]] = {}
        self._tgt_tbls: dict[str, dict[str, None]] = {}
        self._ref_tbls: set[str] = set()
        self._col_edges: dict[str, Tuple[Column, Column]] = {}
        self._tbl_edges: dict[str, Tuple[str, str]] = {}
        self._ref_edges: dict[str, Tuple[str, str]] = {}
        self.__dot: gv.Digraph = None
        self.__nodes: list[str] = []

    @staticmethod
    def merge(nodes: list[Tuple[str, node.Node]]) -> Lineage:
        lineage = Lineage()

        for _nd in nodes:
            nm, nd = _nd[0], _nd[1]
            lineage.__nodes.append(nm)

            summary = nd.summary(nm)

            if (
                not summary.src_tbls
                and not summary.ref_tbls
                and isinstance(nd, node.Select)
            ):
                continue

            for tbl, cols in summary.src_tbls.items():
                lineage._src_tbls.setdefault(tbl, {})
                lineage._src_tbls[tbl].update(cols)

            tbl = next(iter(summary.tgt_tbl.keys()))
            lineage._tgt_tbls.setdefault(tbl, {})
            lineage._tgt_tbls[tbl].update(summary.tgt_tbl[tbl])

            for tbl in summary.ref_tbls:
                if tbl not in lineage._src_tbls:
                    lineage._ref_tbls.add(tbl)

            lineage._col_edges.update(summary.col_edges)
            lineage._tbl_edges.update(summary.tbl_edges)
            lineage._ref_edges.update(
                {
                    k: v
                    for k, v in summary.ref_edges.items()
                    if k not in lineage._tbl_edges
                }
            )

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

        match type:
            case 1:
                self.draw_1()
            case 2:
                self.draw_2()
            case 3:
                self.draw_3()

        self.__dot.render("pglineage")

    def __out_table(self, tbl: str) -> str:
        return "" if tbl.startswith(node.Select.STATEMENT) else tbl

    def __draw_tables(self, tables: dict[str, dict[str, None]], type: int) -> None:
        if type == 1:
            for tbl, flds in tables.items():
                xlabel = self.__out_table(tbl)
                label = ""
                for fld in flds:
                    sep = "|" if label else ""
                    label += sep + "<" + fld + "> " + fld
                self.__dot.node(tbl, shape="record", label=label, xlabel=xlabel)

        elif type in (2, 3):
            for tbl in tables.keys():
                self.__dot.node(tbl, shape="cylinder", label=self.__out_table(tbl))

    def _draw_srctables(self, type: int) -> None:
        self.__draw_tables(self._src_tbls, type)

    def _draw_tgttables(self, type: int) -> None:
        self.__draw_tables(self._tgt_tbls, type)

    def _draw_reftables(self) -> None:
        for rt in self._ref_tbls:
            self.__dot.node(rt, shape="cylinder", label=self.__out_table(rt))

    def _draw_coledges(self) -> None:
        for edge in self._col_edges.values():
            self.__dot.edge(
                edge[0].table + ":" + edge[0].name,
                edge[1].table + ":" + edge[1].name,
                label="",
            )

    def _draw_tbledges(self) -> None:
        for e in self._tbl_edges.values():
            self.__dot.edge(e[0], e[1])

    def _draw_refedges(self) -> None:
        for re in self._ref_edges.values():
            self.__dot.edge(re[0], re[1], style="dashed")

    def _draw_nodes(self) -> None:
        for name in self.__nodes:
            self.__dot.node(name, label=name, shape="note")

    def draw_1(self) -> None:
        self._draw_srctables(1)
        self._draw_tgttables(1)
        self._draw_coledges()

    def draw_2(self) -> None:
        self._draw_srctables(2)
        self._draw_tgttables(2)
        self._draw_nodes()
        self._draw_tbledges()

    def draw_3(self) -> None:
        self._draw_srctables(3)
        self._draw_tgttables(3)
        self._draw_reftables()
        self._draw_nodes()
        self._draw_tbledges()
        self._draw_refedges()
