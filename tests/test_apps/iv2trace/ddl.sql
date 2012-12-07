-- Messages table that stores all the trace messages
CREATE TABLE msgs
(
-- timestamp of when this record is recorded
   ts                timestamp  NOT  NULL
,  action            tinyint    NOT  NULL
,  type              tinyint
,  localhsid         bigint     NOT  NULL
,  sourcehsid        bigint     NOT  NULL
,  cihandle          bigint     NOT  NULL
,  coordhsid         bigint     NOT  NULL
,  txnid             bigint     NOT  NULL
,  sphandle          bigint
,  truncationhandle  bigint
,  ismp              tinyint
,  inseq             tinyint
,  procname          varchar(255)
,  status            tinyint    NOT  NULL
);
PARTITION TABLE msgs ON COLUMN txnid;

CREATE INDEX msgs_txnid_idx ON msgs
(
    txnid
);
CREATE INDEX msgs_type_idx ON msgs
(
    type, txnid, action
);
CREATE INDEX msgs_inseq_idx ON msgs
(
    inseq, txnid, type
);

-- procedures
CREATE PROCEDURE FROM CLASS iv2trace.procedures.AddMsg;
PARTITION PROCEDURE AddMsg ON TABLE msgs COLUMN txnid;
CREATE PROCEDURE iv2trace.procedures.getMsgsForTxn AS
       SELECT * FROM msgs WHERE txnid = ? ORDER BY localhsid, ts;
PARTITION PROCEDURE getMsgsForTxn ON TABLE msgs COLUMN txnid;
