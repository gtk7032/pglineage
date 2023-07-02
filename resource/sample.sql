WITH SELECT c1 FROM t1; SELECT 
c2 FROM t2;

SELECT fnk();

insert into tgt (col1, col2) select col1, col2 from tgt where tgt.x = 2;