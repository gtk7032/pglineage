from .column import Column


class Table:
    def __init__(self, name: str) -> None:
        self.name = name
        self.columns: dict[str, None] = {}

    def update(self, columns: dict[Column | str, None]) -> None:
        for col in columns:
            self.add(col)

    def add(self, column: Column | str) -> None:
        k = column if isinstance(column, str) else column.name
        self.columns.setdefault(k)

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Table):
            return False
        return self.name == __o.name

    def __hash__(self) -> int:
        return hash(self.name)
