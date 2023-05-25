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
        in_tables: dict[str, dict[str, set[str]]] = {}
        out_tables: dict[str, set[str]] = {}

        for nd in nodes:
            ins, out = nd.summary()

            for tbl, cols in ins.items():
                if tbl in in_tables.keys():
                    for col, refs in cols.items():
                        if col in in_tables[tbl].keys():
                            in_tables[tbl][col].update(refs)
                        else:
                            in_tables[tbl][col] = refs
                else:
                    in_tables[tbl] = cols

            out_tbl = next(iter(out.keys()))
            if out_tbl in out_tables.keys():
                out_tables[out_tbl].update(out[out_tbl])
            else:
                out_tables[out_tbl] = set(out[out_tbl])
