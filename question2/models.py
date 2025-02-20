from sqlite3 import connect

def setup_database():
    conn = connect('jokes.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jokes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            type TEXT,
            joke TEXT,
            setup TEXT,
            delivery TEXT,
            nsfw INTEGER,
            political INTEGER,
            sexist INTEGER,
            safe INTEGER,
            lang TEXT
        )
    ''')
    conn.commit()
    conn.close()