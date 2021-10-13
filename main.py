import psycopg2 as pg 
import pandas as pd
import urllib.request
import zipfile
import csv

print("IMPORTING FILES")

url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
extract_dir = "files"

zip_path, _ = urllib.request.urlretrieve(url)
with zipfile.ZipFile(zip_path, "r") as f:
    f.extractall(extract_dir)

print("CONNECTING TO DB")

DBCONFIGFILE = 'credentials.txt'
with open(DBCONFIGFILE) as f:
    dbconfig=f.read()
con=pg.connect(dbconfig)
cur = con.cursor()

print("CREATING TABLES IN DB")

cur.execute('drop table if exists movies, ratings')

create_movies_table='CREATE TABLE if not exists movies \
(movieid integer, \
title character varying, \
genres character varying);'

cur.execute(create_movies_table)
  
create_ratings_table = 'CREATE TABLE  if not exists ratings \
(movieid integer, \
userid integer, \
rating double precision, \
"timestamp" character varying);'

cur.execute(create_ratings_table)

print("IMPORTING CSV TO TABLES IN DB")

with open('files/ml-latest-small/movies.csv', 'r', encoding="UTF-8") as f:
    reader = csv.reader(f, delimiter=',')
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO movies  (movieid,title,genres) VALUES (%s, %s, %s)",
        row
    )

with open('files/ml-latest-small/ratings.csv', 'r', encoding="UTF-8") as f:
    reader = csv.reader(f, delimiter=',')
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO ratings (movieid,userid,rating,timestamp) VALUES (%s, %s, %s, %s)",
        row
    )

con.commit()

print("SENDING SQL QUERIES")
pr1="1. How many movies are in data set?"
q1="select count(distinct title) from movies;"
pr2= "2. What is the most common genre of movie?"
q2="with t1 as \
(select count(genres) as m, genres \
from movies \
where genres not like '%no%' \
group by genres \
order by count(genres) desc) \
select  genres from t1  \
limit 1;"

pr3="3. What are top 10 movies with highest rating?"

q3=" with t1 as \
( \
select rating as r, avg(rating) as a, movieid, count(userid) as c \
	from ratings \
	group by movieid, rating \
) \
select t1.*, m.title from t1 join movies m \
on t1.movieid =m.movieid \
order by t1.r desc, t1.a desc, t1.c desc limit 10;"


pr4="4. What are 5 most often rating users?"

q4="select \
userid, count(userid) \
from ratings \
group by userid \
order by count(userid) desc \
limit 5;"

pr5="5. When was done first and last rate included in data set and what was the rated movie tittle?"
q5="select  \
min(to_timestamp(CAST (timestamp AS INTEGER))) as min_time, \
max(to_timestamp(CAST(timestamp AS INTEGER))) as max_time \
from ratings;"

pr6="6. Find all movies released in 1990"
q6="select distinct movieid, title from \
movies \
where title like '%(1990)';"

tasks=[(pr1,q1),(pr2,q2),(pr3,q3),(pr4,q4),(pr5,q5),(pr6,q6)]

def read_query(con, query):
    cursor = con.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except:
        print("Error")
for pair in tasks:
    result=pd.read_sql(pair[1], con)
    print(pair[0], '\n', result, '\n')

cur.close()
con.close()

print("THE END")