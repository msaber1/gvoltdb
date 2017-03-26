CREATE TABLE Users ( 
                        uId integer default '0' NOT NULL, 
                        lName varchar(16) default NULL, 
                        dob varchar(16) default NULL, 
                        PRIMARY KEY  (uId) 
                        );

CREATE TABLE Relationships (
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
                        EDGES (ID = relId, FROM = uId, TO = uId2, 
                        startDate = sDate, relative = isRelative) 
                        FROM Relationships; 
                        
                        
select * from SocialNetwork.Vertexes;
select * from SocialNetwork.Edges;
select V.FanOut, R.lName from SocialNetwork.Vertexes V JOIN USERS R ON V.ID = R.UID;
