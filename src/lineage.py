from __future__ import annotations

import graphviz as gv

import node
from column import Column


class Lineage:
    def __init__(
        self,
        in_tables: dict[str, dict[str, None]] = {},
        out_tables: dict[str, dict[str, None]] = {},
        dirs: dict[str, dict[str, Column | str]] = {},
    ) -> None:
        self._in_tables = in_tables
        self._out_tables = out_tables
        self._dirs = dirs
        self.__dot: gv.Digraph = None

    @staticmethod
    def merge(nodes: list[node.Node]) -> Lineage:
        lineage = Lineage()

        for nd in nodes:
            ins, out, dirs = nd.summary()

            for tbl, cols in ins.items():
                lineage._in_tables.setdefault(tbl, {})
                lineage._in_tables[tbl].update(cols)

            tbl = next(iter(out.keys()))
            tbl = tbl if tbl else str(len(lineage._out_tables))
            lineage._out_tables.setdefault(tbl, {})
            lineage._out_tables[tbl].update(next(iter(out.values())))

            lineage._dirs.update(dirs)

        return lineage

    @staticmethod
    def create(nodes: list[node.Node]) -> Lineage:
        return Lineage.merge(nodes)

    def draw_process(self) -> None:
        names = {dir["name"] for dir in self._dirs.values()}
        for name in names:
            self.__dot.node(name, label=name, shape="note")

        ps = {}
        for dir in self._dirs.values():
            ps.setdefault(
                dir["from"].table + dir["name"],
                (dir["from"].table, dir["name"]),
            )
            ps.setdefault(
                dir["name"] + dir["to"].table,
                (dir["name"], dir["to"].table),
            )

        for p in ps.values():
            self.__dot.edge(p[0], p[1])

    def draw(self, type: int) -> None:
        self.__dot = gv.Digraph(format="png", filename="pglineage.gv")
        self.__dot.attr("graph", rankdir="LR")
        self.__dot.attr("node", fontname="MS Gothic")

        if type == 1:
            self.draw_1()
        elif type == 2:
            self.draw_2()

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

        draw_tables(self._in_tables)
        draw_tables(self._out_tables)

        for dir in self._dirs.values():
            f, t, n = dir["from"], dir["to"], dir["name"]
            self.__dot.edge(f.table + ":" + f.name, t.table + ":" + t.name, label="")

    def draw_2(self) -> None:
        def draw_tables(tables: dict[str, dict[str, None]]) -> None:
            for tbl in tables.keys():
                self.__dot.node(tbl, shape="cylinder", label=self.out_table(tbl))

        draw_tables(self._in_tables)
        draw_tables(self._out_tables)
        self.draw_process()
