#!/usr/bin/env python3
import os
import sqlite3
from typing import Dict, Any

# Read DB path from environment to allow local overrides and avoid committing DB files
DB_PATH = os.environ.get('PYTHON_DB_PATH', os.path.join(os.path.dirname(__file__), '..', 'data', 'metadata.db'))
DB_DIR = os.path.dirname(DB_PATH)
os.makedirs(DB_DIR, exist_ok=True)

CREATE_TRACKS = """
CREATE TABLE IF NOT EXISTS tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    artists TEXT,
    album TEXT,
    release_date TEXT,
    genre TEXT,
    language TEXT,
    bpm REAL,
    key TEXT,
    explicit INTEGER,
    publisher TEXT,
    identifiers TEXT,
    raw_json TEXT
);
"""

CREATE_WRITERS = """
CREATE TABLE IF NOT EXISTS writers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER,
    name TEXT,
    ipi TEXT,
    role TEXT,
    split REAL,
    FOREIGN KEY(track_id) REFERENCES tracks(id)
);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db():
    conn = get_conn()
    conn.execute(CREATE_TRACKS)
    conn.execute(CREATE_WRITERS)
    conn.commit()
    conn.close()


def save_track(payload: Dict[str, Any]) -> int:
    """
    Save validated payload to DB.
    Returns inserted track_id.
    """
    conn = get_conn()
    cur = conn.cursor()
    identifiers = payload.get('identifiers', {})
    artists = ','.join(payload.get('artists', []))
    cur.execute(
        """
        INSERT INTO tracks (title, artists, album, release_date, genre, language, bpm, key, explicit, publisher, identifiers, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get('title'),
            artists,
            payload.get('album'),
            payload.get('release_date'),
            payload.get('genre'),
            payload.get('language'),
            payload.get('bpm'),
            payload.get('key'),
            int(payload.get('explicit', False)),
            payload.get('publisher'),
            str(identifiers),
            str(payload)
        )
    )
    track_id = cur.lastrowid
    writers = payload.get('songwriters', [])
    for w in writers:
        cur.execute(
            "INSERT INTO writers (track_id, name, ipi, role, split) VALUES (?, ?, ?, ?, ?)",
            (track_id, w.get('name'), w.get('ipi'), w.get('role'), w.get('split'))
        )
    conn.commit()
    conn.close()
    return track_id

# Initialize DB on import (safe: will create DB file at DB_PATH if it doesn't exist)
init_db()