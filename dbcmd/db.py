import os
import psycopg2 as psy

sql = psy.connect(os.environ["DATABASE_URL"])
cur = sql.cursor()

q = "CREATE TABLE IF NOT EXISTS port (Name text, Initial real, Final real, PnL real, BuyBal real, SellBal real, CA text, Followed text)"
cur.execute(q)
cur.execute("ALTER TABLE port ADD COLUMN IF NOT EXISTS Followed text")
print("Table port Created")

q1 = "CREATE TABLE IF NOT EXISTS bal (Balance real)"
cur.execute(q1)
print("Table bal created")

sql.commit()
sql.close()
