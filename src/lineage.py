from __future__ import annotations

import graphviz as gv

import node
from column import Column


class Lineage:
    def __init__(
        self,
    ) -> None:
        self._src_tbls: dict[str, dict[str, None]] = {}
        self._tgt_tbls: dict[str, dict[str, None]] = {}
        self._ref_tbls: dict[str, None] = {}
        self._edges: dict[str, dict[str, Column | str]] = {}
        self._ref_edges: dict[str, dict[str, str]] = {}
        self.__dot: gv.Digraph = None

    @staticmethod
    def merge(nodes: list[node.Node]) -> Lineage:
        lineage = Lineage()

        for nd in nodes:
            srctbls, tgttbls, reftbls, edges, ref_edges = nd.summary()

            for tbl, cols in srctbls.items():
                lineage._src_tbls.setdefault(tbl, {})
                lineage._src_tbls[tbl].update(cols)

            for tbl in reftbls.keys():
                lineage._ref_tbls.setdefault(tbl, {})

            tbl = next(iter(tgttbls.keys()))
            lineage._tgt_tbls.setdefault(tbl, {})
            lineage._tgt_tbls[tbl].update(tgttbls[tbl])

            lineage._edges.update(edges)
            lineage._ref_edges.update(ref_edges)

        return lineage

    @staticmethod
    def create(nodes: list[node.Node]) -> Lineage:
        return Lineage.merge(nodes)

    def draw(self, type: int) -> None:
        self.__dot = gv.Digraph(format="png", filename="pglineage.gv")
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")

        if type == 1:
            self.draw_1()
        elif type == 2:
            self.draw_2()
        elif type == 3:
            self.draw_3()

        self.__dot.render("pglineage")

    def out_table(self, tbl: str) -> str:
        return "" if tbl.startswith(node.Select.STATEMENT) else tbl

    def draw_tables_1(self, tables: dict[str, dict[str, None]]) -> None:
        for tbl, flds in tables.items():
            xlabel = self.out_table(tbl)
            label = ""
            for fld in flds:
                sep = "|" if label else ""
                label += sep + "<" + fld + "> " + fld
            self.__dot.node(tbl, shape="record", label=label, xlabel=xlabel)

    def draw_tables_2_3(self, tables: dict[str, dict[str, None]]) -> None:
        for tbl in tables.keys():
            self.__dot.node(tbl, shape="cylinder", label=self.out_table(tbl))

    def draw_reftables(self) -> None:
        for rt in self._ref_tbls.keys():
            self.__dot.node(rt, shape="cylinder", label=self.out_table(rt))

    def draw_edges_1(self) -> None:
        for edge in self._edges.values():
            f, t = edge["from"], edge["to"]
            self.__dot.edge(f.table + ":" + f.name, t.table + ":" + t.name, label="")

    def draw_edges_2_3(self) -> None:
        names = {dir["name"] for dir in self._edges.values()}
        for name in names:
            self.__dot.node(name, label=name, shape="note")

        edges = {}
        for edge in self._edges.values():
            edges.setdefault(
                edge["from"].table + edge["name"],
                (edge["from"].table, edge["name"]),
            )
            edges.setdefault(
                edge["name"] + edge["to"].table, (edge["name"], edge["to"].table)
            ),

        for e in edges.values():
            self.__dot.edge(e[0], e[1])

    def draw_refedges(self) -> None:
        for re in self._ref_edges.values():
            self.__dot.edge(re["from"], re["to"], style="dashed")

    def draw_1(self) -> None:
        self.draw_tables_1(self._src_tbls)
        self.draw_tables_1(self._tgt_tbls)
        self.draw_edges_1()

    def draw_2(self) -> None:
        self.draw_tables_2_3(self._src_tbls)
        self.draw_tables_2_3(self._tgt_tbls)
        self.draw_edges_2_3()

    def draw_3(self) -> None:
        self.draw_tables_2_3(self._src_tbls)
        self.draw_tables_2_3(self._tgt_tbls)
        self.draw_edges_2_3()
        self.draw_reftables()
        self.draw_refedges()
