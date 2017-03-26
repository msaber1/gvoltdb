CREATE TABLE Courses ( 
   CourseId integer default '0' NOT NULL, 
   Course varchar(16) default NULL, 
   PRIMARY KEY  (CourseId) 
 );
 
 CREATE TABLE Course_Prereq ( 
   PrereqId integer default '0' NOT NULL,
   CourseId integer default '0' NOT NULL, 
   CoursePrereqId integer default NULL, 
   PRIMARY KEY  (PrereqId) 
 );

insert into Courses values (0, 'CS251');
insert into Courses values (1, 'CS381');
insert into Courses values (2, 'CS182');
insert into Courses values (3, 'CS348');
insert into Courses values (4, 'CS448');
insert into Courses values (5, 'CS541');

insert into Course_Prereq values (0, 5, 2);
insert into Course_Prereq values (1, 5, 3);
insert into Course_Prereq values (2, 5, 4);
insert into Course_Prereq values (3, 4, 0);
insert into Course_Prereq values (4, 4, 1);

CREATE DIRECTED GRAPH VIEW CourseDependencyGraph 
VERTEXES (ID = CourseId, CourseNum = Course) 
FROM Courses
EDGES (ID = PrereqId, FROM = CourseId, TO = CoursePrereqId) 
FROM Course_Prereq; 

CREATE TABLE Offerings ( 
   ID integer default '0' NOT NULL,
   Semester varchar(16) default NULL
 );
 
insert into Offerings values (0, 'Fall 2014');
insert into Offerings values (1, 'Fall 2014');
insert into Offerings values (2, 'Spring 2015');
insert into Offerings values (3, 'Fall 2014');
insert into Offerings values (3, 'Spring 2015');
insert into Offerings values (3, 'Fall 2015');
insert into Offerings values (4, 'Fall 2014');
insert into Offerings values (3, 'Spring 2015');
insert into Offerings values (4, 'Spring 2015');
insert into Offerings values (5, 'Spring 2016');

select * from CourseDependencyGraph.Vertexes V JOIN Offerings R ON V.ID = R.ID;
