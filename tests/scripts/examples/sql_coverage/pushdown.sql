-- DML, generate random data first

INSERT INTO P1 VALUES(_id, _value[string], _value[int16], _value[float]);
INSERT INTO R1 VALUES(_id, _value[string], _value[int16], _value[float]);
INSERT INTO R2 VALUES(_id, _value[string], _value[int16], _value[int16]);

-- Distributed limit on a partitioned table
SELECT _variable[@order] FROM _table order by _variable[@order] limit _value[int:0,100];
-- Distributed limit/offset on a partitioned table
SELECT _variable[@order] FROM _table order by _variable[@order] limit _value[int:0,100] offset _value[int:0,100];

-- Distribute a limit on a distinct scan
SELECT distinct(_variable[@order]) FROM _table order by _variable[@order] limit _value[int:0,10];
-- Distribute a limit/offset on a distinct scan
SELECT distinct(_variable[@order]) FROM _table order by _variable[@order] limit _value[int:0,10] offset _value[int:0,10];

-- Combine an aggregate with a limit, both should be pushed down.
SELECT _variable[@order], _agg(_variable[int:0,1000]) from _table group by _variable[@order] order by _variable[@order] limit 4;
-- Combine an aggregate with a limit/offset, both should be pushed down.
SELECT _variable[@order], _agg(_variable[int:0,1000]) from _table group by _variable[@order] order by _variable[@order] limit 4 offset 4;

-- Combine a join of partitioned tables with a pushed-down limit
SELECT P1.ID, R2.R2_ID from P1, R2 where P1.ID < R2.R2_ID order by P1.ID, R2.R2_ID limit 10;
SELECT P1.ID, R2.R2_ID from P1, R2 where P1.ID >= R2.R2_ID order by P1.ID, R2.R2_ID limit 10;
-- Combine a join of partitioned tables with a pushed-down limit/offset
SELECT P1.ID, R2.R2_ID from P1, R2 where P1.ID < R2.R2_ID order by P1.ID, R2.R2_ID limit 10 offset 10;
SELECT P1.ID, R2.R2_ID from P1, R2 where P1.ID >= R2.R2_ID order by P1.ID, R2.R2_ID limit 10 offset 10;
