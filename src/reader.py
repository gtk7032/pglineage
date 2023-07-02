import os
import re
from typing import Tuple


class FileReader:

    def __init__(self) -> None:
        self.p = re.compile(
            "((?:with|select|update|insert|delete).+?;)",
            flags=re.IGNORECASE + re.DOTALL,
        )

    def read(self, path:str)->list[Tuple[str,str]]:
        with open(path,"r") as f:
            s = f.read().lower()
        sqls = self.p.findall(s)
        name, _ = os.path.splitext(os.path.basename(path))
        name=name.lower()
        return [ (name+"-"+str(i+1), sql) for i, sql in enumerate(sqls)]
        
        
