INSERT INTO tbl4 (col1, col2) 
SELECT col1, 'c2' FROM tbl1;

UPDATE tbl4 SET col2 = tbl2.col2 
FROM tbl2, tbl3 
WHERE tbl4.col2 = tbl3.col1;

DELETE FROM tbl4 
WHERE EXISTS 
( 
  SELECT 1 
  FROM tbl3 
  WHERE tbl3.col2 = tbl4.col2
);

SELECT col1, col2, 'c3' 
FROM tbl4;

