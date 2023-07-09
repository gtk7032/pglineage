from pathlib import Path

from analyzer import Analyzer
from reader import FileReader

files = [p for p in Path("resource").glob("**/*") if p.is_file()]

reader = FileReader()
analyzer = Analyzer()

for file in files:
    analyzer.load(reader.read(str(file)))

lineage = analyzer.analyze()
lineage.draw(output="output/result", format="svg")
