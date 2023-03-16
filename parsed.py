from __future__ import annotations

from typing import List

from restarget import ResTarget
from table import Table


class ParsedStatement:
    def __init__(
        self,
        layer: int,
        columns: List[ResTarget],
        tables: List[Table],
        next: List[ParsedStatement],
    ) -> None:
        self.layer = layer
        self.columns = columns
        self.tables = tables
        self.next = next

    def show(self):
        print("----------")
        print("layer：" + str(self.layer))
        for i, col in enumerate(self.columns):
            print("column" + str(i) + ": " + str(col))
        print("tables:" + Table.list2str(self.tables))
        print("----------")
        for res in self.next:
            print("\n")
            res.show()
