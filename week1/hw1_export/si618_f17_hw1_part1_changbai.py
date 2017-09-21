#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 10:27:29 2017

@author: liuchangbai
"""

import json
import sqlite3 as sql


data = open('movie_actors_data.txt','r')
data_json = [json.loads(x) for x in data]

connect = sql.connect('homework.db')
cursor = connect.cursor()


# Get data
imdb_id = [imdb.get('imdb_id') for imdb in data_json]
genre = [g.get('genres') for g in data_json]
title = [title.get('title') for title in data_json]
year = [year.get('year') for year in data_json]
rating = [rate.get('rating') for rate in data_json]
actor = [actor.get('actors') for actor in data_json]



# movie_genre Table
#cursor.execute('IF OBJECT_ID(N'movie_genre',N'U') IS NOT NULL DROP TABLE movie_genre')
cursor.execute('CREATE TABLE movie_genre (imdb_id text, genre text);')

data_list1 = []
for imdb,genre in enumerate(list(zip(imdb_id,genre))):
    for i,j in enumerate(genre[1]):
        data_list1.append((genre[0], j))

cursor.executemany('INSERT INTO movie_genre VALUES (?, ?);', data_list1)
connect.commit()





# movies Table
cursor.execute('CREATE TABLE movies (imdb_id text, title text, year real, rating real);')

data_list2 = list(zip(imdb_id,title,year,rating))

cursor.executemany('INSERT INTO movies VALUES (?, ?, ?, ?);', data_list2)
connect.commit()




# movie_actor Table
cursor.execute('CREATE TABLE movie_actor (imdb_id text, actor text);')

data_list3 = []

for imdb, actor in enumerate(list(zip(imdb_id,actor))):
    for i, j in enumerate(actor[1]):
        data_list3.append((actor[0], j))
        
cursor.executemany('INSERT INTO movie_actor VALUES (?, ?);', data_list3)
connect.commit()




################################ Questions ####################################

# 5. (10 points) Write an SQL query to find top 10 genres with most movies 
# and print out the results 

query1 = cursor.execute('SELECT genre, count(*) as Count FROM movie_genre ' + 
                        'GROUP BY genre ORDER BY Count DESC LIMIT 10;')
print('\n'+'Top 10 genres:','\n','Genre, Movies')
for number,temp in enumerate(query1.fetchall()):
    print(str(temp[0]) + ", " + str(temp[1]))



# 6. (10 points) Write an SQL query to find number of movies broken down by
# year in chronological order 

query2 = cursor.execute('SELECT year, count(*) as Count FROM movies GROUP BY year ORDER BY year;')
print('\n'+'Movies broken down by year:'+'\n'+'Year, Movies')
for number,temp in enumerate(query2.fetchall()):
    print(str(int(temp[0])) + ", " + str(temp[1]))


# 7. (10 points) Write an SQL query to find all Sci-Fi movies order 
# by decreasing rating, then by decreasing year if ratings are the same. 

query3 = cursor.execute('SELECT title, year, rating FROM movies M JOIN movie_genre G ON (M.imdb_id = G.imdb_id)'
                        + 'WHERE G.genre == "Sci-Fi" ORDER BY rating DESC, year DESC;')
print('\n'+'Sci-Fi Movies:'+'\n'+'Title, Year, Rating')
for number,temp in enumerate(query3.fetchall()):
    print(str(temp[0]) + ", " + str(int(temp[1])) + ", " + str(temp[2]))
    
    

# 8. (10 points) Write an SQL query to find the top 10 actors who played 
# in most movies in and after year 2000. In case of ties, sort the rows by actor name. 

query4 = cursor.execute('SELECT actor, count(*) as COUNT FROM movie_actor act LEFT JOIN movies M on M.imdb_id = act.imdb_id '
                        + 'where M.year >= 2000 GROUP BY act.actor ORDER BY COUNT DESC, actor limit 10;')
print('\n'+'In and after year 2000, top 10 actors who played in most movies:' +'\n' +'Actor, Movies')
for number,temp in enumerate(query4.fetchall()):
    print(str(temp[0]) + ", " + str(temp[1]))


# 9. (10 points) Write an SQL query for finding pairs of actors who co-stared 
# in 3 or more movies. The pairs of names must be unique. This means that ‘actor A,
# actor B’ and ‘actor B, actor A’ are the same pair, so only one of them should appear. 

query5 = cursor.execute('SELECT distinct case when a1 < a2 then a1 else a2 end as a1,case when a1 < a2 then a2 else a1 end as a2,'
                        + 'Count from (SELECT a1, a2, count(*) as Count from ('
                        +'SELECT imdb_id, actor as a1 from movie_actor) f left join ('
                        +'SELECT imdb_id, actor as a2 from movie_actor) s ON f.imdb_id = s.imdb_id '
                        +' WHERE a1 != a2 group by 1,2) WHERE Count >= 3 ORDER BY Count DESC, a1, a2;')

print('\n'+'Pairs of actors who co-stared in 3 or more movies:'+'\n'+'Actor A, Actor B, Co-stared Movies')
for number,temp in enumerate(query5.fetchall()):
    print(str(temp[0]) + ", " + str(temp[1]) + ", " + str(temp[2]))
















