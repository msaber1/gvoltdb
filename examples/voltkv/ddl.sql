CREATE TABLE store
(
  key      varchar(250) not null,
  value    varbinary(1048576) default null,
  lock_txnid bigint default null,
  lock_expiration_time bigint default null,
  lock_root_key varchar(250) default null,
  PRIMARY KEY (key)
);

CREATE TABLE journal
(
  key      varchar(250) not null,
  lock_txnid bigint not null,
  lock_expiration_time bigint not null,
  target_key varchar(250) not null,
  set_payload tinyint not null,
  payload    varbinary(1048576) 
);
CREATE INDEX IX_lock_txnid ON journal ( lock_txnid );
CREATE INDEX IX_lock_expiration ON journal ( lock_expiration_time );



