from pathlib import Path

from analyzer import Analyzer
from reader import FileReader

files = [str(p) for p in Path("resource").glob("**/*") if p.is_file()]

reader = FileReader()
analyzer = Analyzer()

for file in files:
    try:
        sqls = reader.read(file)
    except Exception:
        print("file reading error : " + file)
        continue
    analyzer.load(sqls)

lineage = analyzer.analyze()
lineage.draw(output="output/result", format="png")
