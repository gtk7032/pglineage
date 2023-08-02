DELETE FROM table4 
WHERE EXISTS 
( 
  SELECT 1 
  FROM table3 
  WHERE table3.col2 = table4.col2
);
