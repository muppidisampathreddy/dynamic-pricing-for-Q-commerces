import duckdb
import os

class DatabaseManager:
    def __init__(self, db_path="data/blinkit_data.db"):
        self.db_path = db_path
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        conn = duckdb.connect(self.db_path)
        # We use CREATE TABLE IF NOT EXISTS here for production stability, 
        # but since user wants a fresh schema refactor we could drop if needed.
        # Given the previous context, I'll keep it simple.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                scraped_at TIMESTAMP,
                keyword VARCHAR,
                product_id BIGINT,
                merchant_id BIGINT,
                product_name VARCHAR,
                brand VARCHAR,
                price FLOAT,
                mrp FLOAT,
                unit_value FLOAT,
                unit_type VARCHAR,
                inventory INTEGER,
                unavail_qty INTEGER,
                rating FLOAT
            )
        """)
        conn.close()

    def save_dataframe(self, df):
        if df is None or df.is_empty():
            return
        conn = duckdb.connect(self.db_path)
        conn.execute("INSERT INTO price_history SELECT * FROM df")
        conn.close()

    def get_row_count(self):
        conn = duckdb.connect(self.db_path)
        count = conn.execute("SELECT count(*) FROM price_history").fetchone()[0]
        conn.close()
        return count

    def export_csv(self, filename="data/blinkit_export.csv"):
        conn = duckdb.connect(self.db_path)
        conn.execute(f"COPY price_history TO '{filename}' (HEADER, DELIMITER ',')")
        conn.close()
        print(f"Data exported to {filename}")
