import csv
import psycopg2

def connect():
    conn_string = "host='localhost' dbname='postgres' user='postgres' password='pizza'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    
    with open('data/signals.csv', "r") as fin:
        reader = csv.reader(fin)
        fieldnames = next(reader)
        cursor.copy_from(fin, 'signals', columns=fieldnames, sep=",")
        conn.commit()
        conn.close()