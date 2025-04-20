import psycopg2
import pandas as pd

# --- Database connection details ---
hostname = "localhost"
database = "final"  # <-- your database name
username = "postgres"
port_id = 5432
password = "admin"

csv_file = "/mnt/data/NIFTY SME EMERGE_Historical_PR_01122016to20042025.csv"  # Correct path

# --- Column Mapping (if needed) ---
# Suppose your CSV has columns like: Date, Open, High, Low, Close, Volume
column_map = {
    "Date": "date",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume"
}

try:
    # 1. Load CSV
    df = pd.read_csv(csv_file)
    df.rename(columns=column_map, inplace=True)
    df.columns = df.columns.str.strip()  # Clean any extra spaces

    print("Loaded Data:")
    print(df.head())
    print("Columns:", df.columns)

    # 2. Connect to PostgreSQL
    conn = psycopg2.connect(
        host=hostname, dbname=database, user=username, password=password, port=port_id
    )
    cur = conn.cursor()

    # 3. Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nifty_sme_emerge (
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # 4. Insert data into table
    insert_query = """
    INSERT INTO nifty_sme_emerge (date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s);
    """

    records = df.to_records(index=False)
    cur.executemany(insert_query, records)
    conn.commit()

    print("✅ Data uploaded successfully!")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
