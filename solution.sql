CREATE TABLE users (
userid INTEGER PRIMARY KEY, 
name TEXT NOT NULL
);

CREATE TABLE movies (
	movieid INTEGER PRIMARY KEY,
	title TEXT NOT NULL
);

CREATE TABLE taginfo (
	tagid INTEGER PRIMARY KEY, 
	content TEXT NOT NULL
);

CREATE TABLE genres (
	genreid INTEGER PRIMARY KEY, 
	name TEXT NOT NULL
);

CREATE TABLE ratings (
	userid INTEGER, 
	movieid INTEGER, 
	rating NUMERIC NOT NULL, 
	CHECK (rating BETWEEN 0 AND 5),
	timestamp BIGINT NOT NULL, 
	PRIMARY KEY (userid, movieid),
	FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
	FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE
);

CREATE TABLE tags (
	userid INTEGER, 
	movieid INTEGER, 
	tagid INTEGER, 
	timestamp BIGINT NOT NULL, 
	PRIMARY KEY (userid, movieid, tagid),
	FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
	FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE,
	FOREIGN KEY (tagid) REFERENCES taginfo ON DELETE CASCADE
);

CREATE TABLE hasagenre(
	movieid INTEGER,
	genreid INTEGER, 
	PRIMARY KEY (movieid, genreid), 	
	FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE, 
	FOREIGN KEY (genreid) REFERENCES genres ON DELETE CASCADE	
);