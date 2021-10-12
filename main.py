import psycopg2 as pg 
import pandas as pd

DBCONFIGFILE = 'credentials.txt'
with open(DBCONFIGFILE) as f:
    dbconfig=f.read()
con=pg.connect(dbconfig)
'''def create_server_connection(credentials):
    con = None
    try:
		with open(credentials) as f:
			dbconfig=f.read()
        con = pg.connect(dbconfig)
		
        print("PgSQL Database connection successful")
    except:
        print("Error")

    return connection
create_server_connection(DBCONFIGFILE)'''

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
    results = read_query(con, pair[1])
    df=pd.DataFrame(results)
    print(pair[0], df)
  

