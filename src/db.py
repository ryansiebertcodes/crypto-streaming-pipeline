import os
import psycopg2

def get_connection():
    try:
        conn = psycopg2.connect(
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
        )
        print("Connection established successfully!")
        return conn

    except Exception as e:
        print("Connection failed! Error=", {e})
        return None
    