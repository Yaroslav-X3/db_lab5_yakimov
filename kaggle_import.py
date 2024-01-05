import csv
import decimal
import psycopg2

username = 'yakimov'
password = '1029nRiEg3847'
database = 'db_lab5'

INPUT_CSV_FILE = 'data.csv'
#-- GAME --
game_drop = '''
DROP TABLE IF EXISTS Game CASCADE
'''
game_create = '''
CREATE TABLE Game
(
  game_id SERIAL NOT NULL,
  game_link CHAR(200) NOT NULL,
  game_name CHAR(200) NOT NULL,
  age_rating CHAR(10),
  platform_name CHAR(100) NOT NULL,
  PRIMARY KEY (game_id)
);
'''

game_insert = '''
INSERT INTO Game (game_link, game_name, age_rating, platform_name)
VALUES (%s, %s, %s, %s)
RETURNING game_id
'''
#-- GENRE --
genre_drop = '''
DROP TABLE IF EXISTS Genre CASCADE
'''

genre_create = '''
CREATE TABLE Genre
(
  genre_id SERIAL NOT NULL,
  genre_name CHAR(100) NOT NULL,
  PRIMARY KEY (genre_id)
)
'''
genre_insert = '''
INSERT INTO Genre (genre_name) VALUES (%s) RETURNING genre_id
'''
#-- GAME_GENRE --
game_genre_drop = '''
DROP TABLE IF EXISTS Game_genre
'''
game_genre_create = '''
CREATE TABLE Game_genre
( 
  game_id INT NOT NULL,
  genre_id INT NOT NULL,
  PRIMARY KEY (game_id, genre_id),
  FOREIGN KEY (game_id) REFERENCES Game(game_id),
  FOREIGN KEY (genre_id) REFERENCES Genre(genre_id)
)
'''
game_genre_insert = '''
INSERT INTO Game_genre(game_id, genre_id) VALUES (%s, %s)
'''
#-- DEVELOPER --
dev_drop = '''
DROP TABLE IF EXISTS Developer CASCADE
'''

dev_create = '''
CREATE TABLE Developer
(
  dev_id SERIAL NOT NULL,
  dev_name CHAR(100) NOT NULL,
  PRIMARY KEY (dev_id)
)
'''
dev_insert = '''
INSERT INTO Developer (dev_name) VALUES (%s) RETURNING dev_id
'''
#-- GAME_DEVELOPER --
game_dev_drop = '''
DROP TABLE IF EXISTS Game_developer
'''
game_dev_create = '''
CREATE TABLE Game_developer
( 
  game_id INT NOT NULL,
  dev_id INT NOT NULL,
  PRIMARY KEY (game_id, dev_id),
  FOREIGN KEY (game_id) REFERENCES Game(game_id),
  FOREIGN KEY (dev_id) REFERENCES Developer(dev_id)
)
'''
game_dev_insert = '''
INSERT INTO Game_developer(game_id, dev_id) VALUES (%s, %s)
'''
#-- META RATING --
m_r_drop = '''
DROP TABLE IF EXISTS Meta_rating
'''
m_r_create = '''
CREATE TABLE Meta_rating
( 
  game_id INT NOT NULL,
  meta_score FLOAT,
  user_score FLOAT,
  PRIMARY KEY (game_id),
  FOREIGN KEY (game_id) REFERENCES Game(game_id)
)
'''
m_r_insert = '''
INSERT INTO Meta_rating(game_id, meta_score, user_score) VALUES (%s, %s, %s)
'''
#-- DATE --
date_drop = '''
DROP TABLE IF EXISTS Date
'''
date_create = '''
CREATE TABLE Date
( 
  game_id INT NOT NULL,
  date CHAR(20) NOT NULL,
  PRIMARY KEY (game_id),
  FOREIGN KEY (game_id) REFERENCES Game(game_id)
)
'''
date_insert = '''
INSERT INTO Date(game_id, date) VALUES (%s, %s)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

def convert_to_set(array_str):
    splitted = array_str[1:-1].split(', ')
    return set(x[1:-1] for x in splitted)

with conn:
    cur = conn.cursor()
    cur.execute(game_drop)
    cur.execute(genre_drop)
    cur.execute(game_genre_drop)
    cur.execute(dev_drop)
    cur.execute(game_dev_drop)
    cur.execute(m_r_drop)
    cur.execute(date_drop)
    cur.execute(genre_create)
    cur.execute(game_create)
    cur.execute(game_genre_create)
    cur.execute(dev_create)
    cur.execute(game_dev_create)
    cur.execute(m_r_create)
    cur.execute(date_create)
    
    genres_id = {}
    devs_id = {}
    m_ss_id = {}
    u_ss_id = {}
    
    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf)
        
        for row in reader:
            
            values = (
                row['link'],
                row['title'],
                row['esrb_rating'],
                row['platform'],
            )
            
            cur.execute(game_insert, values)
            game_id = cur.fetchone()[0]
            
            genres = convert_to_set(row['genres'])
            for genre in genres:
                genre_id = genres_id.get(genre)
                if genre_id is None:
                    # insert genre into genre table
                    cur.execute(genre_insert, (genre,))
                    genre_id = cur.fetchone()[0]
                    genres_id[genre] = genre_id
                # insert game-genre relationship to game_genre table
                cur.execute(game_genre_insert, (game_id, genre_id))

            developers = convert_to_set(row['developers'])
            for dev in developers:
                dev_id = devs_id.get(dev)
                if dev_id is None:
                    # insert dev into dev table
                    cur.execute(dev_insert, (dev,))
                    dev_id = cur.fetchone()[0]
                    devs_id[dev] = dev_id
                # insert game-dev relationship to game_dev table
                cur.execute(game_dev_insert, (game_id, dev_id))

            m_s = float(row['meta_score']) if row['meta_score']!='' else None
            
            u_s = float(row['user_score']) if row['user_score']!='' else None

            cur.execute(m_r_insert, (game_id, m_s, u_s))

            date = row['date']

            cur.execute(date_insert, (game_id, date))

    conn.commit()
