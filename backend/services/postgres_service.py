import psycopg2
from config import *

def get_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST
    )

def save_user(name, skills):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO users (name) VALUES (%s) RETURNING id",
        (name,)
    )

    user_id = cur.fetchone()[0]

    for skill in skills:
        cur.execute(
            "INSERT INTO skills (user_id, skill) VALUES (%s, %s)",
            (user_id, skill)
        )

    conn.commit()
    cur.close()
    conn.close()