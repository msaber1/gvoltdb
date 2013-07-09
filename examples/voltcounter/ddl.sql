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
, level BIGINT default 0
, description  varchar(1024) 
, counter_value BIGINT default 0
, rollup_seconds INTEGER default 0
, last_update_time TIMESTAMP NOT NULL
, PRIMARY KEY (counter_id)
);

-- counter class mapping table
CREATE TABLE counter_maps
(
  counter_class_id BIGINT NOT NULL
, counter_id   BIGINT NOT NULL
, PRIMARY KEY (counter_id)
);

CREATE TABLE counter_rollups
(
  rollup_id varchar(256) NOT NULL
, rollup_value BIGINT NOT NULL
, rollup_time TIMESTAMP NOT NULL
);

PARTITION TABLE counter_class ON COLUMN counter_class_id;
PARTITION TABLE counters ON COLUMN counter_class_id;
PARTITION TABLE counter_maps ON COLUMN counter_id;
PARTITION TABLE counter_rollups ON COLUMN rollup_id;

-- Housekeeping queries.
CREATE PROCEDURE FROM CLASS voltcounter.procedures.CleanCounters;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.InitializeClass;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.InitializeRollup;

CREATE PROCEDURE FROM CLASS voltcounter.procedures.GetCounterClass;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.GetCounter;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.UpdateRollups;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.GetCounterStdDev;

CREATE PROCEDURE FROM CLASS voltcounter.procedures.AddCounter;
CREATE PROCEDURE FROM CLASS voltcounter.procedures.Increment;
