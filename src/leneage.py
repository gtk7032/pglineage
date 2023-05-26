from __future__ import annotations

from typing import Tuple

from graphviz import Digraph

import node
from column import Column

dot = Digraph(format="png")
dot.attr("node", fontname="MS Gothic")

dot.node("項目1", shape="record", label="<lp> 左| 中央 |<rp> 右")
dot.node("項目2")
dot.node("項目3")
# エッジ作成
dot.edge("項目1:lp", "項目2")
dot.edge("項目1:rp", "項目3")
dot.render("graphviz-test25")


class Lineage:
    def __init__(
        self,
        in_tables: dict[str, set[str]] = {},
        out_tables: dict[str, set[str]] = {},
        dirs: dict[int, Tuple[Column, Column]] = {},
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
                lineage._in_tables.setdefault(tbl, set())
                lineage._in_tables[tbl].update(cols)

            tbl = next(iter(out.keys()))
            tbl = tbl if tbl else str(len(lineage._out_tables[tbl]))
            lineage._out_tables.setdefault(tbl, set())
            lineage._out_tables[tbl].update(out[tbl])

            lineage._dirs.update(dirs)

        return lineage

    @staticmethod
    def create(nodes: list[node.Node]) -> Lineage:
        return Lineage.merge(nodes)

    def draw(self, type: int) -> None:
        pass
