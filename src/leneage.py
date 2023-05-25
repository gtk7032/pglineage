from __future__ import annotations

from graphviz import Digraph

import node

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
    def __init__(self) -> None:
        pass

    @staticmethod
    def create(nodes: list[node.Node]) -> Lineage:
        pass
