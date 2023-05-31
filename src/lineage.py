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
        self._ref_tbls: dict[str, dict[str, None]] = {}
        self._edges: dict[str, dict[str, Column | str]] = {}
        self.__dot: gv.Digraph = None

    @staticmethod
    def merge(nodes: list[node.Node]) -> Lineage:
        lineage = Lineage()

        for nd in nodes:
            ins, out, dirs = nd.summary()

            for tbl, cols in ins.items():
                lineage._src_tbls.setdefault(tbl, {})
                lineage._src_tbls[tbl].update(cols)

            tbl = next(iter(out.keys()))
            lineage._tgt_tbls.setdefault(tbl, {})
            lineage._tgt_tbls[tbl].update(out[tbl])

            lineage._edges.update(dirs)

        def is_used_in_edge(tgt: str) -> bool:
            is_used = False
            for edge in lineage._edges.values():
                if tgt == edge["from"].table:
                    is_used = True
                    break
            return is_used

        lineage._ref_tbls = {
            tbl: cols
            for tbl, cols in lineage._src_tbls.items()
            if not is_used_in_edge(tbl)
        }
        lineage._src_tbls = {
            tbl: cols
            for tbl, cols in lineage._src_tbls.items()
            if tbl not in lineage._ref_tbls.keys()
        }
        for tbl, cols in lineage._ref_tbls.items():
            for nd in nodes:
                _, _, dirs = nd.summary()
                if not len([True for dir in dirs.values() if dir["from"].table == tbl]):
                    lineage._edges[nd.name + tbl] = {
                        "name": nd.name,
                        "from": tbl,
                        "to": "",
                    }
        return lineage

    @staticmethod
    def create(nodes: list[node.Node]) -> Lineage:
        return Lineage.merge(nodes)

    def draw_edges(self) -> None:
        names = {dir["name"] for dir in self._edges.values()}
        for name in names:
            self.__dot.node(name, label=name, shape="note")

        edges = {}
        for edge in self._edges.values():
            if isinstance(edge["from"], str):
                edges.setdefault(
                    edge["from"] + edge["name"],
                    (edge["from"], edge["name"],True),
            )
            else:
                edges.setdefault(
                    edge["from"].table + edge["name"],
                    (edge["from"].table, edge["name"],False)
            ),
            
            if not edge["to"]:
                continue

            if isinsta
            t = edge["to"] if isinstance(edge["to"], str) else edge["to"].table
            edges.setdefault(
                edge["name"] + t,
                (edge["name"], t),
            )

        for e in edges.values():
            self.__dot.edge(e[0], e[1])
            print(e[0], e[1])

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

    def draw_1(self) -> None:
        def draw_tables(tables: dict[str, dict[str, None]]) -> None:
            for tbl, flds in tables.items():
                xlabel = self.out_table(tbl)
                label = ""
                for fld in flds:
                    sep = "|" if label else ""
                    label += sep + "<" + fld + "> " + fld
                self.__dot.node(tbl, shape="record", label=label, xlabel=xlabel)

        draw_tables(self._src_tbls)
        draw_tables(self._tgt_tbls)

        for edge in self._edges.values():
            f, t, n = edge["from"], edge["to"], edge["name"]
            self.__dot.edge(f.table + ":" + f.name, t.table + ":" + t.name, label="")

    def draw_tables(self, tables: dict[str, dict[str, None]]) -> None:
        for tbl in tables.keys():
            self.__dot.node(tbl, shape="cylinder", label=self.out_table(tbl))

    def draw_2(self) -> None:
        self.draw_tables(self._src_tbls)
        self.draw_tables(self._tgt_tbls)
        self.draw_edges()

    def draw_3(self) -> None:
        self.draw_tables(self._src_tbls)
        self.draw_tables(self._ref_tbls)
        self.draw_tables(self._tgt_tbls)
        self.draw_edges()
