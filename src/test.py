# coding: utf-8
from graphviz import Digraph

dot = Digraph(format="png")
# フォント設定
dot.attr("node", fontname="MS Gothic")
# ノード作成
dot.node("項目1", shape="record", label="<lp> 左| 中央 |<rp> 右")
dot.node("項目2")
dot.node("項目3")
# エッジ作成
dot.edge("項目1:lp", "項目2")
dot.edge("項目1:rp", "項目3")
dot.render("graphviz-test25")
