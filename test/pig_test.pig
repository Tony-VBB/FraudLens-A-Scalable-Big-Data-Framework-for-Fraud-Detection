data = LOAD '/home/balaji/FraudLens/test/sample.csv'
USING PigStorage(',')
AS (step:int, type:chararray, amount:double);

DUMP data;
