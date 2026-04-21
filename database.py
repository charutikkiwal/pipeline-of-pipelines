import sqlite3

DB_NAME = "pipeline_execution.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS execution_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT,
            start_time TEXT,
            end_time TEXT,
            runtime REAL,
            status TEXT,
            output_location TEXT,
            error_message TEXT
        )
    """)

    conn.commit()
    conn.close()


def insert_log(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO execution_logs 
        (pipeline_name, start_time, end_time, runtime, status, output_location, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()