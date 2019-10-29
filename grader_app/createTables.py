import sqlite3
import constants

con = sqlite3.connect(constants.DATABASE_NAME) 
with con:
    cursor = con.cursor()

    cursor.execute('''CREATE TABLE sent (
                    batch INTEGER  PRIMARY KEY NOT NULL,
                    timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime'))
                    )''')

    cursor.execute('''CREATE TABLE results (
                    batch INTEGER  PRIMARY KEY NOT NULL,
                    receivedTimestamp DATETIME,
                    inputTimestamp INTEGER,
                    detected INTEGER,
                    eventTimestamp INTEGER
                    )''')

    
    cursor.execute('''CREATE TABLE expected (
                    inputTimestamp INTEGER,
                    detected INTEGER,
                    eventTimestamp INTEGER
                    )''')