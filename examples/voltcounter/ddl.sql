-- counter class
CREATE TABLE counter_class
(
  counter_class_id BIGINT NOT NULL
, counter_class_desc VARCHAR(255)
, PRIMARY KEY (counter_class_id)
);

-- counter table
CREATE TABLE counters
(
  counter_class_id BIGINT NOT NULL
, counter_id   BIGINT NOT NULL
, parent_id BIGINT NOT NULL
, description  varchar(1024)
, counter_value BIGINT default 0
, rollup_seconds INTEGER default 0
, last_update_time TIMESTAMP NOT NULL
, PRIMARY KEY (counter_id)
);

-- map of counter to all its ancestors.
-- when a counter is bumped all its ancestors are bumped
CREATE TABLE counter_map
(
  counter_id BIGINT NOT NULL
, counter_class_id BIGINT NOT NULL
, parent_id BIGINT NOT NULL
, map_id varchar(256) NOT NULL
, PRIMARY KEY (map_id)
);

-- For each counter a rollup value is kept based on its rollup interval.
CREATE TABLE counter_rollups
(
  rollup_id varchar(256) NOT NULL
, rollup_value BIGINT NOT NULL
, rollup_time TIMESTAMP NOT NULL
, counter_id BIGINT NOT NULL
, counter_class_id BIGINT NOT NULL
);

CREATE VIEW v_counter_rollups
(
  rollup_id
, counter_class_id
, counter_id
, rollup_time
, rollup_count
)
AS
  SELECT rollup_id
       , counter_class_id
       , counter_id
       , rollup_time
       , COUNT(*)
  FROM counter_rollups
  GROUP BY rollup_id
         , counter_class_id
         , counter_id
         , rollup_time
; 

CREATE INDEX counter_map_idx ON counter_map (counter_id, counter_class_id);
CREATE UNIQUE INDEX counter_map_uidx ON counter_map (parent_id, counter_id, counter_class_id);
CREATE INDEX counter_rollup_cid_idx ON counter_rollups (counter_id);
CREATE INDEX counter_rollup_idx ON counter_rollups (rollup_id, rollup_time);
CREATE INDEX counter_id_idx ON counters (counter_id);
CREATE UNIQUE INDEX counter_id_uidx ON counters (counter_class_id, counter_id);

PARTITION TABLE counter_class ON COLUMN counter_class_id;
PARTITION TABLE counters ON COLUMN counter_class_id;
PARTITION TABLE counter_rollups ON COLUMN counter_class_id;
PARTITION TABLE counter_map ON COLUMN counter_class_id;

-- Housekeeping queries.
CREATE PROCEDURE FROM CLASS voltcounter.procedures.CleanCounters;

-- Counter Class
CREATE PROCEDURE FROM CLASS voltcounter.procedures.AddCounterClass;

-- Counter stored procedures.
CREATE PROCEDURE FROM CLASS voltcounter.procedures.AddCounter;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.Increment;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.GetCounterStdDev;
