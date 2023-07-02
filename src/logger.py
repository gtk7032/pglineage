import csv
from typing import NamedTuple


class Row(NamedTuple):
    name: str
    result: str
    query: str


class Logger:

    rows: dict[str, Row]={}

    def set(self, name:str, row:Row)->None:
        Logger.rows[name]=row
    
    def __sort(self)->None:
        Logger.rows = dict(sorted(Logger.rows.items()))
        print(Logger.rows)
    
    def write(self, path:str)->None:
        self.__sort()
        with open(path, "w", encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["name", "status", "query"])
            for r in Logger.rows.values():
                writer.writerow([r.name,r.result,r.query[:100]+" ..."])
