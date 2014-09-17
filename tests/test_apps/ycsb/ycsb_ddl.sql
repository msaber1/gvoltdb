CREATE TABLE Store
(
    keyspace VARCHAR(128)    NOT NULL
,   key      VARCHAR(128)    NOT NULL
,   value    VARBINARY(2056) NOT NULL
);
PARTITION TABLE Store ON COLUMN key;
CREATE UNIQUE INDEX store_pk_index ON Store (keyspace, key);

CREATE UNIQUE INDEX store_hash_index ON Store (keyspace, key);

CREATE PROCEDURE FROM CLASS com.procedures.Put;
PARTITION PROCEDURE Put ON TABLE Store COLUMN key PARAMETER 1;

CREATE PROCEDURE FROM CLASS com.procedures.Scan;
PARTITION PROCEDURE Scan ON TABLE Store COLUMN key PARAMETER 1;

CREATE PROCEDURE com.procedures.simple.Get AS
    SELECT value FROM Store WHERE keyspace = ? AND key = ?
;
PARTITION PROCEDURE Get ON TABLE Store COLUMN key PARAMETER 1;