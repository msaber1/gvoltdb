CREATE TABLE Users (  
uId integer default '0' NOT NULL,  
lName varchar(16) default NULL,  
dob varchar(16) default NULL,  
PRIMARY KEY  (uId)  
);

CREATE VIEW V_USERS AS
SELECT * FROM USERS;
