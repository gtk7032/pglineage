from __future__ import annotations

from typing import List

from restarget import ResTarget


class ParsedStatement:
    def __init__(
        self,
        layer: int,
        columns: List[ResTarget],
        tables: List[str],
        next: List[ParsedStatement],
    ) -> None:
        self.layer = layer
        self.columns = columns
        self.tables = tables
        self.next = next

    def show(self):
        print("----------")
        print(f"{self.layer=}")
        for col in self.columns:
            col.show()
        print(f"{self.tables=}")
        print("----------")
        for res in self.next:
            print("\n")
            res.show()
