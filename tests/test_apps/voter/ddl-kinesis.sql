CREATE STREAM audit_artifact PARTITION ON COLUMN INSTANCE_ID
EXPORT TO TARGET audit_artifact
( 
  SEQ                BIGINT,
  INSTANCE_ID        BIGINT NOT NULL,
  EVENT_TYPE_ID      INTEGER,
  EVENT_DATE         TIMESTAMP default now,
  TRANS              VARCHAR(1000)
);

CREATE STREAM business_artifact PARTITION ON COLUMN INSTANCE_ID
EXPORT TO TARGET business_artifact
( 
  SEQ                BIGINT,
  INSTANCE_ID        BIGINT NOT NULL,
  EVENT_TYPE_ID      INTEGER,
  EVENT_DATE         TIMESTAMP default now,
  TRANS              VARCHAR(2024)
);

CREATE STREAM RT_Metrics_MDP PARTITION ON COLUMN INSTANCE_ID
EXPORT TO TARGET RT_Metrics_MDP
( 
  SEQ                BIGINT,
  INSTANCE_ID        BIGINT NOT NULL,
  EVENT_TYPE_ID      INTEGER,
  EVENT_DATE         TIMESTAMP default now,
  TRANS              VARCHAR(70)
);

CREATE STREAM RT_Metrics_TTE PARTITION ON COLUMN INSTANCE_ID
EXPORT TO TARGET RT_Metrics_TTE
( 
  SEQ                BIGINT,
  INSTANCE_ID        BIGINT NOT NULL,
  EVENT_TYPE_ID      INTEGER,
  EVENT_DATE         TIMESTAMP default now,
  TRANS              VARCHAR(52)
);

LOAD classes voter-procs.jar;
-- The following CREATE PROCEDURE statements can all be batched.
CREATE PROCEDURE partition on table RT_Metrics_TTE column INSTANCE_ID FROM CLASS voter.InsertTTE;
CREATE PROCEDURE partition on table RT_Metrics_MDP column INSTANCE_ID FROM CLASS voter.InsertMDP;
CREATE PROCEDURE partition on table business_artifact column INSTANCE_ID FROM CLASS voter.InsertBiz;
CREATE PROCEDURE partition on table audit_artifact column INSTANCE_ID FROM CLASS voter.InsertAudit;
