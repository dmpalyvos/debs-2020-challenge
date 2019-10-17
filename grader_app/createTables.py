import sqlite3
import state

con = sqlite3.connect(state.DB_NAME)
with con:
    cursor = con.cursor()

    cursor.execute('''CREATE TABLE sent (
                    batch INTEGER  PRIMARY KEY NOT NULL,
                    timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime'))
                    )''')

    cursor.execute('''CREATE TABLE received (
                    batch INTEGER  PRIMARY KEY NOT NULL,
                    timestamp DATETIME,
                    result TEXT
                    )''')