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

    def draw(self, type: int) -> None:
        dot = gv.Digraph(format="png", filename="pglineage.gv")
        dot.attr("graph", rankdir="LR")
        dot.attr("node", fontname="MS Gothic")

        def draw_tables(tables: dict[str, dict[str, None]]) -> None:
            for tbl, flds in tables.items():
                xlabel = "" if tbl.startswith(node.Select.STATEMENT) else tbl
                label = ""
                for fld in flds:
                    sep = "|" if label else ""
                    label += sep + "<" + fld + "> " + fld
                    dot.node(tbl, shape="record", label=label, xlabel=xlabel)

        draw_tables(self._in_tables)
        draw_tables(self._out_tables)

        # names = {dir["name"] for dir in self._dirs.values()}
        # for name in names:
        #     dot.node(name, label=name)

        for dir in self._dirs.values():
            f, t, n = dir["from"], dir["to"], dir["name"]
            dot.edge(f.table + ":" + f.name, t.table + ":" + t.name, label=n)
            # dot.edge(f.table + ":" + f.name, n)
            # dot.edge(n, t.table + ":" + t.name)

        dot.render("pglineage")
