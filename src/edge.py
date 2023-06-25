from typing import NamedTuple

from column import Column


class ColEdge(NamedTuple):
    tail: Column
    head: Column


class TblEdge(NamedTuple):
    tail: str
    head: str
