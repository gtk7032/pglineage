from __future__ import annotations

from typing import Any

import node


class Table:
    def __init__(self, ref: str | "node.Select") -> None:
        self.ref = ref

    def format(self) -> str | dict[str, Any]:
        return self.ref if isinstance(self.ref, str) else self.ref.format()
