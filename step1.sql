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

DR TABLE Courses;
DR TABLE Course_Prereq;
