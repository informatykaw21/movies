#syntax=docker/dockerfile:1
FROM python:3.8-slim-buster
WORKDIR /app
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2
RUN pip3 install pandas
COPY credentials.txt credentials.txt
COPY main.py main.py
CMD ["python3", "main.py"]