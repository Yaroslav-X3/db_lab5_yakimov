import json

import psycopg2

username = 'yakimov'
password = '1029nRiEg3847'
database = 'db_lab4'
host = 'localhost'
port = '5432'

connection = psycopg2.connect(user=username, password=password, dbname=database, host=host)

data={}

with connection.cursor() as cursor:
    tables = ['date', 'developer', 'game', 'game_developer', 'game_genre', 'genre', 'meta_rating']
    for t in tables:
        cursor.execute(f'select * from {t}')
        fields=[x[0] for x in cursor.description]
        rows=[dict(zip(fields, row)) for row in cursor]
        data[t]=rows
    with open('data.json', 'w') as file:
        json.dump(data,file,default=str)
