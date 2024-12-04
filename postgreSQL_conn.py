import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="noraayaz",
        user="noraayaz",
        password="your_new_password"
    )
    print("Test connection successful!")
    conn.close()
except Exception as e:
    print(f"Test connection failed: {e}")
