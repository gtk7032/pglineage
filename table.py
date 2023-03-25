from __future__ import annotations

from typing import Any, Dict


class Table:
    def __init__(self, name: str, ref) -> None:
        from parsed import ParsedStatement
        self.name = name
        self.ref: str | ParsedStatement = ref

    def __str__(self) -> str:
        return self.ref if isinstance(self.ref, str) and self.ref else self.name

    def format(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ref": self.ref if isinstance(self.ref, str) else self.ref.format()
        }

