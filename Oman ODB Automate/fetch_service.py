#!/usr/bin/env python3
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from pathlib import Path

# DB config for local forwarded port
DB_HOST = '0.0.0.0'
DB_PORT = 1234
DB_USER = 'XYZ'        # your MySQL Workbench username
DB_PASS = 'XYZ'        # your MySQL Workbench password

# ===== User-defined parameters =====
START_DATE = '2025-05-01'
END_DATE   = '2025-05-15'

# ===== Dynamically build table name based on START_DATE =====
month_str = datetime.strptime(START_DATE, '%Y-%m-%d').strftime('%Y%m')
TABLE_NAME = f"ivr_mastercalllogs_{month_str}"

# ===== Build query dynamically =====
BASE_QUERY = f"""
SELECT DISTINCT(b_party)
FROM `cdrlog_backup_archive`.`{TABLE_NAME}`
WHERE DATE(start_datetime) BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND call_status='failure'
  AND call_type='outgoing';
"""


DND_QUERY = """
SELECT DISTINCT(msisdn) FROM `subs_engine0`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine1`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine2`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine3`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine4`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine5`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine6`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine7`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine8`.`subscription` UNION
SELECT DISTINCT(msisdn) FROM `subs_engine9`.`subscription`
UNION
SELECT DISTINCT(msisdn) FROM ivr_data.`ivr_blacklist`
UNION
SELECT DISTINCT(msisdn) FROM global.`blacklisted`;
"""

BASE_FILE = Path("base.txt")
DND_FILE = Path("dnd.txt")

def fetch_and_save(query: str, output_file: Path, append=False):
    """Fetch data from DB and save to a file."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS
        )

        if conn.is_connected():
            print(f"✅ Connected to MySQL at {DB_HOST}:{DB_PORT}")
            
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print(f"⚠️ No records found for {output_file.name}")
            return

        mode = 'a' if append else 'w'
        with output_file.open(mode, encoding="utf-8") as f:
            for row in results:
                f.write(f"{row[0]}\n")

        print(f"✅ Saved {len(results)} records to {output_file.resolve()} (append={append})")

    except Error as e:
        print(f"❌ DB connection/query error: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    # Save failed outgoing calls to base.txt (overwrite)
    fetch_and_save(BASE_QUERY, BASE_FILE, append=False)
    
    # Save subscriptions & blacklists to dnd.txt (append if exists)
    fetch_and_save(DND_QUERY, DND_FILE, append=True)
