CREATE DATABASE movies;

USE movies;

CREATE TABLE imdb (
    imdb_id VARCHAR(16) PRIMARY KEY,
    vote_average FLOAT,
    vote_count INT,
    release_date DATE,
    revenue DECIMAL(15, 2),
    budget DECIMAL(15, 2),
    runtime INT
); 

CREATE TABLE movie_directors (
    imdb_id VARCHAR(16),
    director_name VARCHAR(255),
    PRIMARY KEY (imdb_id, director_name),
    FOREIGN KEY (imdb_id) REFERENCES imdb(imdb_id)
);