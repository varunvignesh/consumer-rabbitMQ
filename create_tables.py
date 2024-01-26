import os, sqlite3
from dotenv import load_dotenv

# Get the .env variables
load_dotenv()

# Initialize connection
conn = sqlite3.connect(os.getenv("DATABASE_NAME"))

# Create table product_db
conn.execute("""
    CREATE TABLE IF NOT EXISTS product_db (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        index_number INTEGER,
        available_price CHAR(20),
        stock CHAR(20),
        source CHAR(20),
        reference_product_id CHAR(200)
    )
    """)

# Create Indexes for the product_db potential query columns
conn.execute("""
    CREATE INDEX product_db_index_number_idx ON product_db (index_number)
             """)

conn.execute("""
    CREATE INDEX product_db_stock_idx ON product_db (stock)
             """)

conn.execute("""
    CREATE INDEX product_db_reference_product_id_idx ON product_db (reference_product_id)
             """)

# Create table meta_info_db
conn.execute("""
    CREATE TABLE IF NOT EXISTS meta_info_db (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_info_id INTEGER,
        account_code CHAR(256),
        crawl_page_counter INTEGER,
        postal_zip_code CHAR(10),
        postal_zip_name CHAR(256),
        store_code CHAR(256),
        place_name TEXT,
        admin_name CHAR(256),
        bundle_versions_row_pk_hash TEXT,
        major_version_end_time DATETIME,
        bundle_variant_field_mapping json,
        bundle_definition BLOB,
        fulfilment_modes BLOB,
        seller_name CHAR(256),
        bundle_match_type TEXT,
        bundle_definition_hash TEXT,
        FOREIGN KEY(product_info_id) REFERENCES ProductInfo(id)
    )
    """)

# Create Indexes for the meta_info_db potential query columns
conn.execute("""
    CREATE INDEX meta_info_db_account_code_idx ON meta_info_db (account_code)
             """)

conn.execute("""
    CREATE INDEX meta_info_db_bundle_match_type_idx ON meta_info_db (bundle_match_type)
             """)

print("Created all the tables and indexes successfully")

# close connection
conn.close()