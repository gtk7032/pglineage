from __future__ import annotations

from analyzer import Analyzer

# from pprint import pprint
from lineage import Lineage

# sql = "select a, b, c from tbl;"
# sql = (
#     "insert into to_table as totbl (to_col1, to_col2) "
#     "select 5 * from_col1, from_col2 from from_table;"
# )
# sql = "insert into to_table (to_col1, to_col2)"
# sql = "select 5 * from_table1.from_col1, from_table2.from_col4 from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = (
#     "insert into to_table (to_col1, to_col2)"
#     "select case when true then from_table1.from_col1 when false then from_table2.from_col4 else 5 end from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"
# )

# sql = "INSERT INTO new_table ( col1, col2, col3 ) WITH tmp_table AS ( SELECT col1 as colX, col2, col3, 5 FROM old_table ) SELECT col1, col2, col3, 4 FROM tmp_table"

# sql = "SELECT s1.age * s2.age as al, s1.age_count * 5, 5, 'aa' FROM ( SELECT age, COUNT(age) as age_count FROM students as stu GROUP BY age ) as s1, s2;"

sql = "SELECT tbl1.res AS res2, tbl2.res, tbl3.res FROM tbl1 INNER JOIN tbl2 ON tbl1.col = tbl2.col, tbl3 WHERE tbl1.col2 = tbl3.col2;"

# sql = (
#     "with get_top5_amount_id as ("
#     "select customer_id as id, "
#     "sum(amount) sum_amount, "
#     "xxx "
#     "from payment "
#     ") "
#     "select email, get_top5_amount_id.xxx "
#     "from customer, get_top5_amount_id, "
#     "(select * from tbl ) as tbl2 "
#     "join get_top5_amount_id "
#     "on customer.customer_id = get_top5_amount_id.id;"
# )
# sql = "UPDATE EMPLOYEES SET SALARY = 8500 WHERE LAST_NAME = 'Keats';"


if __name__ == "__main__":
    analyzer = Analyzer()
    analyzer.load(sql, "hello")
    nodes = analyzer.analyze()
    # for nd in nodes:
    #     pprint(nd._flatten().format())
    lineage = Lineage.create(nodes)
    lineage.draw(2)
