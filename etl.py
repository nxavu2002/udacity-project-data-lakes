import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 into staging tables (staging_songs, staging_events) on Redshift using the queries in `copy_table_queries` list.
        * cur --    reference to connected db.
        * conn --   parameters (host, dbname, user, password, port)
                    to connect the DB.

    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into fact and dimension tables on Redshift using the queries in `insert_table_queries` list.
    
        * cur --    reference to connected db.
        * conn --   parameters (host, dbname, user, password, port)
                    to connect the DB.

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()