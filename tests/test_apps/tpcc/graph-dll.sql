CREATE TABLE Users (  
uId integer default '0' NOT NULL,  
lName varchar(16) default NULL,  
dob varchar(16) default NULL,  
PRIMARY KEY  (uId)  
);

CREATE TABLE Ralationships (  
relId integer default '0' NOT NULL,  
uId integer default '0' NOT NULL,  
uId2 integer default '0' NOT NULL,  
isRelative integer default NULL,  
sDate varchar(16) default NULL,  
PRIMARY KEY  (relId)  
);

CREATE UNDIRECTED GRAPH VIEW SocialNetwork 
VERTEXES (ID = uId, lstName = lName, birthdat = dob) 
FROM Users 
WHERE 1 = 1 
EDGES (ID = relId, FROM = uId, TO = uId2, 
startDate = sDate, relative = isRelative) 
FROM Ralationships 
WHERE 1 = 1;
