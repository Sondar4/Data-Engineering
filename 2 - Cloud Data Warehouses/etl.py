import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sys import exit


def load_staging_tables(cur, conn):
    """Loads data from the JSON files on S3 onto the staging tables.
    """
    for query in copy_table_queries:
        print("- Loading data into staging table {}".format(query.split(" ")[5]))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Loads data from the staging tables onto the analytic tables.
    """
    for query in insert_table_queries:
        print("- Inserting data from staging into table {}".format(query.split(" ")[6]))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection established.")
    except Exception as e:
        print("Something went wrong trying to connect to the cluster:")
        print(e)
        exit()

    try:
        load_staging_tables(cur, conn)
        print('Staging tables loaded.')
        insert_tables(cur, conn)
        print('Analytic tables created.')
    except Exception as e:
        print("Something went wrong inserting data in tables:")
        print(e)

    conn.close()


if __name__ == "__main__":
    main()