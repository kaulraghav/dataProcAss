--Answer 1) 
CREATE TABLE query1 AS
SELECT g.name, COUNT(h.movieid) AS moviecount
FROM genres g, hasagenre h
WHERE g.genreid = h.genreid
GROUP BY G.genreid;

--Answer 2) 
CREATE TABLE query2 AS
SELECT g.name, AVG(r.rating) AS rating
FROM genres g, ratings r, hasagenre h
WHERE g.genreid = h.genreid AND h.movieid = r.movieid
GROUP BY g.name;

--Answer 3) 
CREATE TABLE query3 AS
SELECT m.title, COUNT(r.rating) AS countofratings
FROM movies m, ratings r
WHERE m.movieid = r.movieid
GROUP BY m.title
HAVING COUNT(r.rating) >= 10;

--Answer 4) 
CREATE TABLE query4 AS
SELECT m.movieid, m.title
FROM movies m, hasagenre h, genres g
WHERE h.genreid = g.genreid AND m.movieid = h.movieid
GROUP BY m.movieid, g.name
HAVING g.name = 'Comedy'; 

--Answer 5) 
CREATE TABLE query5 AS
SELECT m.title, AVG(r.rating) AS average
FROM movies m, ratings r
WHERE m.movieid = r.movieid
GROUP BY m.title;	

--Answer 6) 
CREATE TABLE query6 AS
SELECT AVG(r.rating) AS average
FROM movies m, ratings r, hasagenre h, genres g
WHERE h.genreid = g.genreid AND m.movieid = r.movieid AND h.movieid = m.movieid
GROUP BY g.name
HAVING g.name = 'Comedy';

--Answer 7) 
CREATE TABLE query7 AS
SELECT AVG(r.rating) AS average 
FROM ratings r 
WHERE r.movieid IN 
((SELECT h.movieid 
  FROM genres g, hasagenre h   
  WHERE g.genreid = h.genreid AND g.name = 'Comedy') 
 INTERSECT 
 (SELECT h.movieid 
  FROM genres g, hasagenre h   
  WHERE g.genreid = h.genreid AND g.name = 'Romance'));

--Answer 8) 
CREATE TABLE query8 AS
SELECT AVG(r.rating) AS average 
FROM ratings r 
WHERE r.movieid IN 
((SELECT h.movieid 
  FROM genres g, hasagenre h   
  WHERE g.genreid = h.genreid AND g.name = 'Romance') 
 EXCEPT 
 (SELECT h.movieid 
  FROM genres g, hasagenre h   
  WHERE g.genreid = h.genreid AND g.name = 'Comedy'));

--Answer 9) 
CREATE TABLE query9 AS 
SELECT movieid, rating from ratings 
WHERE userid = :v1;

--Answer 10) 
--Average Ratings 
CREATE TABLE averagetable AS 
SELECT movieid, AVG(rating) AS average
FROM ratings
GROUP BY movieid;

--Similarity Table 
CREATE TABLE simil AS (
SELECT i.movieid AS movieid1, l.movieid AS movieid2, ( 1 - ( ABS ( i.average - l.average)) / 5) as sim, q.rating, m.title
FROM averagetable i, averagetable l, query9 q, movies m
WHERE i.movieid NOT IN (SELECT movieid FROM query9) 
AND l.movieid IN (SELECT movieid FROM query9)
AND l.movieid = q.movieid 
AND i.movieid = m.movieid);

--Final Recommendation Table
CREATE TABLE recommendation AS (
SELECT title
FROM simil 
GROUP BY title, movieid1
HAVING (SUM (sim * rating) /SUM(sim)) > 3.9);




