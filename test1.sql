CREATE TABLE C1 ( 
	CourseId integer default '0' NOT NULL, 
	Course varchar(16) default NULL, 
	PRIMARY KEY  (CourseId) 
);
 
CREATE TABLE C2 ( 
	PrereqId integer default '0' NOT NULL,
	CourseId integer default '0' NOT NULL, 
	CoursePrereqId integer default NULL, 
	PRIMARY KEY  (PrereqId) 
);

insert into C1 values (0, 'CS251');
insert into C1 values (1, 'CS381');
insert into C1 values (2, 'CS182');
insert into C1 values (3, 'CS348');
insert into C1 values (4, 'CS448');
insert into C1 values (5, 'CS541');

insert into C2 values (0, 5, 2);
insert into C2 values (1, 5, 3);
insert into C2 values (2, 5, 4);
insert into C2 values (3, 4, 0);
insert into C2 values (4, 4, 1);

CREATE DIRECTED GRAPH
VIEW CourseDependencyGraph 
VERTEXES (ID = CourseId, CourseNum = Course) 
	FROM C1
EDGES (ID = PrereqId, FROM = CourseId, TO = CoursePrereqId) 
	FROM C2;

select * from CourseDependencyGraph.Vertexes;
