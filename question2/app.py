from fastapi import FastAPI, HTTPException
import requests
import sqlite3

app = FastAPI()

# Database setup
DATABASE = 'jokes.db'

def setup_database():
    conn = sqlite3.connect(DATABASE)
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

# Fetch jokes from JokeAPI
def fetch_jokes():
    url = "https://v2.jokeapi.dev/joke/Any?amount=100"
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch jokes")
    return response.json()['jokes']

# Store jokes in the database
def store_jokes(jokes):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    for joke in jokes:
        cursor.execute('''
            INSERT INTO jokes (category, type, joke, setup, delivery, nsfw, political, sexist, safe, lang)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            joke['category'],
            joke['type'],
            joke.get('joke', ''),
            joke.get('setup', ''),
            joke.get('delivery', ''),
            int(joke['flags']['nsfw']),
            int(joke['flags']['political']),
            int(joke['flags']['sexist']),
            int(joke['safe']),
            joke['lang']
        ))
    conn.commit()
    conn.close()

# API endpoint to fetch and store jokes
@app.get("/fetch-jokes")
def fetch_and_store_jokes():
    setup_database()
    jokes = fetch_jokes()
    store_jokes(jokes)
    return {"message": "Jokes fetched and stored successfully"}

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)