CREATE DIRECTED GRAPH
VIEW CourseDependencyGraph 
VERTEXES (ID = CourseId, CourseNum = Course) 
	FROM Courses
EDGES (ID = PrereqId, FROM = CourseId, TO = CoursePrereqId) 
	FROM Course_Prereq;

select * from CourseDependencyGraph.Vertexes;
