import psycopg2

username = 'yakimov'
password = '1029nRiEg3847'
database = 'db_lab4'
host = 'localhost'
port = '5432'

connection = psycopg2.connect(user=username, password=password, dbname=database, host=host)

with connection.cursor() as cursor:
    tables = ['date', 'developer', 'game', 'game_developer', 'game_genre', 'genre', 'meta_rating']
    for t in tables:
        query=f'copy (select * from {t}) to stdout with csv header'
        with open(f'{t}.csv', 'w') as file:
            cursor.copy_expert(query, file)
    
