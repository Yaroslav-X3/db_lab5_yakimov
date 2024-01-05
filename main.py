import matplotlib.pyplot as plt
import matplotlib.dates
import numpy as np
import pandas as pd
import datetime
import time
import re
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="db_lab5",
    user="yakimov",
    password="1029nRiEg3847",
    port="5432")

def plt_(l_1, l_2, l_3):

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)

    names = [scr[0].strip() for scr in l_1]

    #-- MAKE DATA --

    data = {}
    
    data["Meta-рахунок"] = [scr[1] if scr[1] != None else 0 for scr in l_1]

    data["Користувацький рахунок"] = [scr[2]*10 if scr[2] != None else 0 for scr in l_1]

    #-- PLOT --

    x=np.arange(len(names))

    bar_ax.barh(x-0.25,data["Meta-рахунок"],height=0.33,label="Meta-рахунок")
    bar_ax.barh(x+0.25,data["Користувацький рахунок"],height=0.33,label="Користувацький рахунок")
    
    bar_ax.set_yticks([i for i in range(len(names))],names,rotation=45,size=6)

    bar_ax.set_title("Рейтинги Meta та кориcтувачів")

    bar_ax.legend(loc="lower left")

    names = [scr[0].strip() for scr in l_2]

    #-- MAKE DATA --
    
    x = [scr[1] for scr in l_2]

    #-- PLOT --
    
    pie_ax.pie(x, radius=3, center=(4, 4), frame=True, labels=[" "]*len(names), textprops={'fontsize': 6})

    pie_ax.legend(names,title="Розробники",
        loc='upper center',
        bbox_to_anchor=(0.5,0.2),
        fontsize="6")

    #pie_ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
           #ylim=(0, 8), yticks=np.arange(1, 8))

    pie_ax.set_xticks([])
    pie_ax.set_yticks([])

    pie_ax.set_title("Кількість ігор, випущених кожним розробником")

    months = {"Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12"}

    lst_2 = []

    for row in l_3:

        if (re.search(r"\d{4}",row[1].strip()) != None and re.search(r"\D{3}(?!$)",row[1].strip()) != None):
            
            y = re.search(r"\d{4}",row[1].strip()).group()
            m = months[re.search(r"\D{3}(?!$)",row[1].strip()).group()]

            lst_2.append((row[0], f"{y}-{m}"))

    dates = set(time.mktime(datetime.datetime.strptime(str(scr[1]), "%Y-%m").timetuple()) for scr in lst_2)
    genres = set(scr[0].strip() for scr in lst_2)

    #-- MAKE DATA --
    
    x = list(pd.date_range(datetime.datetime.strptime("1995", "%Y"), periods=348, freq='M'))
    y = {}

    n = 0
    
    for genre in genres:
        y[genre] = [sum(scr[0].strip() == genre
            and time.mktime(datetime.datetime.strptime(str(scr[1]), "%Y-%m").timetuple())
            == time.mktime(datetime.datetime.strptime(str(i)[0:-12], "%Y-%m").timetuple()) for scr in lst_2) for i in x]
        y[genre] = list(np.cumsum(y[genre]))
        graph_ax.plot(x, [i + 0.01 * n for i in y[genre]])
        n += 1

    #-- PLOT --

    graph_ax.legend(list(genres),fontsize="6")

    graph_ax.set_title("Кількість ігор за жанром залежно від часу")

    plt.show()

with conn:

    cur=conn.cursor()

    #-- ЗАПИТ 1 --
    
    cur.execute(
    """
    drop view if exists GamesMetaRatings;
    create view GamesMetaRatings as
	select trim(game_name), meta_score, user_score from game cross join meta_rating
	where game.game_id=meta_rating.game_id and game.game_id < 25;

    select * from GamesMetaRatings;
    """)

    l_1=[row for row in cur]

    print(l_1)

    #-- ЗАПИТ 2 --

    cur.execute(
    """
    drop view if exists DevelopersGamesNum;
    create view DevelopersGamesNum as
        select dev_name, count(dev_name) from developer cross join game_developer
            where developer.dev_id = game_developer.dev_id and developer.dev_id < 25
            group by dev_name;
                    
    select * from DevelopersGamesNum;
    """)

    l_2=[row for row in cur]

    print(l_2)

    #-- ЗАПИТ 3 --
    
    cur.execute(
    """
    drop view if exists GenresByYear;

    create view GenresByYear as
    select genre_name, date from date cross join
        (select * from game_genre cross join genre
                where game_genre.genre_id = genre.genre_id) as t
                where t.game_id = date.game_id and t.game_id < 25;
                            
    select * from GenresByYear;
    """)

    l_3=[row for row in cur]

    print(l_3)

    plt_(l_1,l_2,l_3)
