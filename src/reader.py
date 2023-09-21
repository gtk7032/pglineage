import os
import re
from typing import Tuple

from chardet import detect


class FileReader:
    def __init__(self) -> None:
        self.__p0 = re.compile("('.*?);(.*?')")
        self.__p1 = re.compile(
            "(?:with|select|update|insert|delete).+?;", flags=re.DOTALL
        )
        self.__p2 = re.compile("--.*")
        self.__p3 = re.compile("/\*.*?\*/", flags=re.DOTALL)

    def __detect_enc(self, path: str) -> str | None:
        with open(path, "rb") as f:
            b = f.read()
            e = detect(b)
            return e["encoding"]

    def read(self, path: str) -> list[Tuple[str, str]]:
        enc = self.__detect_enc(path)
        if not enc:
            raise Exception()
        with open(path, "r", encoding=enc) as f:
            s = f.read().lower()
        s = self.__p0.sub(r"\1\2", s)
        s = self.__p2.sub("", s)
        s = self.__p3.sub("", s)
        sqls = self.__p1.findall(s)
        name, _ = os.path.splitext(os.path.basename(path))
        name = name.lower()
        return [(name + "-" + str(i + 1), sql) for i, sql in enumerate(sqls)]
