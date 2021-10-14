# movies
Version: 1.0.0
Movies <br>
To make it work, you need to have installed your local postgresql database on port 5432.<br>
Write your credentials for your DB in file title credentials.txt:<br>
dbname=YourDatabaseName user=postgres password=YourPassword host=host.docker.internal<br>
# To use 
In powershell\unix console use these commands:
```
docker build --tag movies .
docker run movies
```

