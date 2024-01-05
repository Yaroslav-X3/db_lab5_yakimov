drop table if exists Game cascade;

CREATE TABLE Game
(
  game_name CHAR(100) NOT NULL,
  age_rating CHAR(10) NOT NULL,
  platform_name CHAR(100) NOT NULL,
  game_link CHAR(200) NOT NULL,
  game_id SERIAL NOT NULL,
  PRIMARY KEY (game_id)
);

drop table if exists Genre cascade;

CREATE TABLE Genre
(
  genre_id SERIAL NOT NULL,
  genre_name CHAR(50) NOT NULL,
  PRIMARY KEY (genre_id)
);

drop table if exists Developer cascade;

CREATE TABLE Developer
(
  dev_id SERIAL NOT NULL,
  dev_name CHAR(100) NOT NULL,
  PRIMARY KEY (dev_id)
);

drop table if exists Meta_rating cascade;

CREATE TABLE Meta_rating
(
  meta_score FLOAT,
  user_score FLOAT,
  game_id INT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES Game(game_id)
);

drop table if exists Date cascade;

CREATE TABLE Date
(
  date CHAR(20) NOT NULL,
  game_id INT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES Game(game_id)
);

drop table if exists Game_genre cascade;

CREATE TABLE Game_genre
(
  genre_id INT NOT NULL,
  game_id INT NOT NULL,
  FOREIGN KEY (genre_id) REFERENCES Genre(genre_id),
  FOREIGN KEY (game_id) REFERENCES Game(game_id)
);

drop table if exists Game_developer cascade;

CREATE TABLE Game_developer
(
  dev_id INT NOT NULL,
  game_id INT NOT NULL,
  FOREIGN KEY (dev_id) REFERENCES Developer(dev_id),
  FOREIGN KEY (game_id) REFERENCES Game(game_id)
);