import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


connection = psycopg2.connect(user="postgres", password="********")
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


cursor = connection.cursor()

cursor.execute('create database data_hh')
cursor.close()
connection.close()
