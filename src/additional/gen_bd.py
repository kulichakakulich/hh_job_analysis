import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database(username, password):
    try:
        with psycopg2.connect(user=f"{username}", password=f"{password}") as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cursor = conn.cursor()
            create_database_query = 'CREATE DATABASE data_hh;'
            cursor.execute(create_database_query)
            print("Database created successfully.")

    except (psycopg2.Error, Exception) as error:
        print(f"Error occurred: {error}")
