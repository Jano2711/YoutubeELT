from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    """
    Establishes a connection to the Postgres database and returns a cursor.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db')
    conn = pg_hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    """
    Closes the cursor and connection to the Postgres database.
    """
    cur.close()
    conn.close()

def create_schema(schema):
    """
    Creates a schema in the Postgres database if it does not exist.
    """
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def create_table(schema):
    """
    Creates a table in the Postgres database based on the provided SQL statement.
    """
    conn, cur = get_conn_cursor()
    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Type" VARCHAR(10) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    """
    Retrieves all video IDs from the specified schema and table.
    """
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Video_ID"] for row in ids]
    return video_ids