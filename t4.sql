CREATE TABLE Users ( 
                        uId integer default '0' NOT NULL, 
                        lName varchar(16) default NULL,  
                        PRIMARY KEY  (uId) 
                        );


CREATE TABLE Relationships (
                        relId integer default '0' NOT NULL, 
                        uId integer default '0' NOT NULL, 
                        uId2 integer default '0' NOT NULL, 
                        isRelative integer default NULL, 
                        PRIMARY KEY  (relId) 
                        );

insert into Users values (0, 'User 0');
insert into Users values (1, 'User 1');
insert into Users values (2, 'User 2');
insert into Users values (3, 'User 3');
insert into Users values (4, 'User 4');

insert into Relationships values (0, 0, 1, 0);
insert into Relationships values (1, 0, 2, 0);
insert into Relationships values (2, 0, 3, 0);
insert into Relationships values (3, 0, 4, 0);
insert into Relationships values (4, 1, 2, 0);
insert into Relationships values (5, 1, 3, 0);
insert into Relationships values (6, 2, 3, 0);

Create Table UserAddress(uId integer default '0' NOT NULL, 
                        address varchar(30) default NULL);

insert into UserAddress values (0, 'User 0 Address');
insert into UserAddress values (1, 'User 1 Address');
insert into UserAddress values (1, 'User 1 Second Address');
insert into UserAddress values (2, 'User 2 Address');
insert into UserAddress values (3, 'User 3 Address');
insert into UserAddress values (4, 'User 4 Address');
                        
CREATE UNDIRECTED GRAPH VIEW SocialNetwork 
                        VERTEXES (ID = uId, lstName = lName) 
                        FROM Users 
                        EDGES (ID = relId, FROM = uId, TO = uId2, 
                        relative = isRelative) 
                        FROM Relationships;

select * from SocialNetwork.Vertexes;
