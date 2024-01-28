import os, sqlite3
from dotenv import load_dotenv

# Get the .env variables
load_dotenv()

# Initialize connection
conn = sqlite3.connect(os.getenv("DATABASE_NAME"))

# Note: added only relevant fields required for score computation.
# Create table account_db
conn.execute("""
    CREATE TABLE IF NOT EXISTS account_db (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_code CHAR(256) NOT NULL UNIQUE
    )
    """)

# Create Indexes for the account_db potential query columns
conn.execute("""
    CREATE INDEX account_db_account_code_idx ON account_db (account_code)
             """)

# Create table product_db
conn.execute("""
    CREATE TABLE IF NOT EXISTS product_db (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER,
        reference_product_id CHAR(200) NOT NULL UNIQUE,
        stock CHAR(20),
        FOREIGN KEY(account_id) REFERENCES account_db(id)
    )
    """)

# Create Indexes for the product_db potential query columns
conn.execute("""
    CREATE INDEX product_db_stock_idx ON product_db (stock)
             """)

conn.execute("""
    CREATE INDEX product_db_reference_product_id_idx ON product_db (reference_product_id)
             """)

print("Created all the tables and indexes successfully")

# close connection
conn.close()