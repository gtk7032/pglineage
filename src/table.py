from __future__ import annotations

from typing import Any

import node


class Table:
    def __init__(self, alias: str, ref: str | "node.Select") -> None:
        self.alias = alias
        self.ref = ref

    def __str__(self) -> str:
        if self.alias:
            return self.alias
        elif isinstance(self.ref, str) and self.ref:
            return self.ref
        else:
            return str(self.format())

    def format(self) -> str | dict[str, Any]:
        return (
            self.alias
            if self.alias
            else self.ref
            if isinstance(self.ref, str)
            else self.ref.format()
        )
