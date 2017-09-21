#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 11:47:38 2017

@author: liuchangbai
"""


import sys
import json
import sqlite3 as sql

movie_type = str(sys.argv[1])
top = int(sys.argv[2])

data = open('movie_actors_data.txt','r')
data_json = [json.loads(x) for x in data]


connect = sql.connect('homework.db')
cursor = connect.cursor()


query = cursor.execute("SELECT actor, count(*) as Count FROM movie_genre G JOIN movie_actor act ON g.imdb_id = act.imdb_id where G.genre == '%s' GROUP BY act.actor order by Count DESC, actor limit %d;" % (movie_type, top))


print("Top %d actors who played in most %s movies:" % (top, movie_type))
print("Actor, %s Movies Played in" % movie_type)

for number,temp in enumerate(query.fetchall()):
    print(str(temp[0]) + ", " + str(temp[1]))


