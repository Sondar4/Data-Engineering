import configparser
import psycopg2
from sys import exit
from os import remove

if __name__=="__main__":
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        remove('error_log.txt')
        remove('detail_error_log.txt')
    except Exception as e:
        print(e)

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection established.")
    except Exception as e:
        print("Something went wrong trying to connect to the cluster:")
        print(e)
        exit()

    try:
        # Get history of all Amazon Redshift load errors
        cur.execute("SELECT * FROM stl_load_errors;")
        with open("error_log.txt", "w") as log:
            for row in cur:
                log.write(str(row))
            print("historic log saved as 'error_log.txt'")

        # Get additional details of the loading error
        cur.execute("SELECT * FROM stl_loaderror_detail;")
        with open("detail_error_log.txt", "w") as log:
            for row in cur:
                log.write(str(row))
            print("log with error details saved as 'detail_error_log.txt'")
    except Exception as e:
        print("Log files could not be written")
        print(e)
    
    conn.close()