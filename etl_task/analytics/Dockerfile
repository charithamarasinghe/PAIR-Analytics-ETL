FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && apt-get -y install python3-pymysql \
    && pip install psycopg2 sqlalchemy \
    && pip install PyMySQL
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD [ "python", "./analytics.py" ]