import asyncio
from flask import Flask, render_template, url_for, jsonify
from flask import request 
from flask_cors import CORS
import psycopg2
from psycopg2 import pool
import requests
import json
import os
from functools import partial
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler

DBURL = os.environ["DATABASE_URL"]

db_pool = psycopg2.pool.ThreadedConnectionPool(1,80,DBURL)
position_executor = ThreadPoolExecutor(max_workers=20)

def get_db():
    return db_pool.getconn()

def return_db(conn):
    db_pool.putconn(conn)

def check_all_positions():
    """Submit all open positions for price checks."""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT CA, Name, Initial FROM port WHERE SellBal IS NULL")
            positions = cursor.fetchall()
    finally:
        return_db(conn)

    for position in positions:
        position_executor.submit(check_position, position)
    
    


def getprices(items):
    l = []
    for i in items:
        print(i)
        response = requests.get(f'https://api.dexscreener.com/tokens/v1/solana/{i["ca"]}',headers={"Accept":"*/*"})
        data = list(response.json())
        print(data)
        # l.append(data[0]["fdv"])
    return l

def getitems():
    sql = get_db()
    try:
        cursor = sql.cursor()
        cursor.execute("SELECT Name,Initial,CA FROM port")
        rec = list(cursor.fetchall())
        cursor.close()
        l=[]
        for i in rec[-1:-11:-1]:
            l.append({"tick":i[0],"fdv":i[1],"ca":i[2]})
        return l
    finally:
        return_db(sql)

    

def getbal():
    sql = get_db()
    try:
        cursor = sql.cursor()
        cursor.execute("SELECT Balance FROM bal")
        rec = list(cursor.fetchall())
        cursor.close()
        bal = rec[0][0]
        return bal
    finally:
        return_db(sql)

    



def fetch():
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    parameters = {
    'symbol': 'ETH,SOL,BTC',
    'convert': 'USD'
    }
    l = {}
    res = requests.get(url, params=parameters,headers={'X-CMC_PRO_API_KEY': "18cc6530935245e48a2327569ff067f9"})
    data = res.json()
    for i in data['data']:
        l[i.lower()] = round(data['data'][i]['quote']['USD']['price'],1)
    return l

def hfetch(curr):
    response = requests.get('https://data-api.coindesk.com/spot/v1/historical/days',
    params={"market":"kraken","instrument":f"{curr}-USD","limit":10,"aggregate":1,"fill":"true","apply_mapping":"true","response_format":"JSON","api_key":"3318b81f3e11391668abf4c54800baf9067c6c643921c68ae47628b18aa4e131"},
    headers={"Content-type":"application/json; charset=UTF-8"}
)
    data = response.json()
    return data

def check_position(position):
    ca, tick, initial_fdv = position

    try:
        response = requests.get(
            f"https://api.dexscreener.com/tokens/v1/solana/{ca}",
            headers={"Accept": "*/*"},
            timeout=10,
        )
        response.raise_for_status()
        pairs = response.json()
        if not pairs:
            return

        final_fdv = pairs[0]["fdv"]
        if final_fdv < 2 * initial_fdv:
            return

        profit = 0.1 * (final_fdv - initial_fdv) / initial_fdv
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE port
                    SET Final = %s, SellBal = COALESCE(SellBal, 0) + %s
                    WHERE CA = %s AND SellBal IS NULL
                    """,
                    (final_fdv, profit, ca),
                )
                if cursor.rowcount:
                    cursor.execute("UPDATE bal SET Balance = Balance + %s", (profit,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db(conn)
    except requests.RequestException as error:
        print(f"Price check failed for {ca}: {error}")
    
        

app = Flask(__name__)
CORS(app)

scheduler = BackgroundScheduler()
scheduler.add_job(
    func=check_all_positions,
    trigger="interval",
    minutes=30,
    max_instances=1,
    coalesce=True,
)
scheduler.start()

@app.route('/')
def main():
        return 'what youre probably looking for is https://frontend-muai.onrender.com/'

@app.post('/helius')
def helius():
    transactions = request.get_json(silent=True) or []
    processed = 0
    seen_addresses = set()

    for transaction in transactions:
        for transfer in transaction.get('tokenTransfers', []):
            ca = transfer.get('mint')
            if not ca or not ca.endswith('pump') or ca in seen_addresses:
                continue
            seen_addresses.add(ca)

            try:
                response = requests.get(
                    f'https://api.dexscreener.com/tokens/v1/solana/{ca}',
                    headers={'Accept': '*/*'},
                    timeout=10,
                )
                response.raise_for_status()
                data = response.json()
            except (requests.RequestException, ValueError) as error:
                print(f'Unable to fetch token data for {ca}: {error}')
                continue

            if not data:
                continue

            token = data[0]
            tick = token.get('baseToken', {}).get('symbol')
            fdv = token.get('fdv')
            native_transfers = transaction.get('nativeTransfers', [])
            amount = native_transfers[0].get('amount', 0) / 1e9 if native_transfers else 0

            if not tick or fdv is None or fdv <= 80000 or amount <= 1:
                continue

            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT 1 FROM port WHERE CA = %s', (ca,))
                    if cursor.fetchone():
                        continue

                    cursor.execute(
                        """
                        INSERT INTO port (Name, Initial, CA, BuyBal)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (tick, fdv, ca, -0.1),
                    )
                    cursor.execute('UPDATE bal SET Balance = Balance - %s', (0.1,))
                conn.commit()
                processed += 1
            except Exception:
                conn.rollback()
                raise
            finally:
                return_db(conn)

    return jsonify({'received': True, 'processed': processed})

    
                
        

@app.route('/bot')
async def bot():
    loop = asyncio.get_event_loop()
    balance, items, (btc,eth,sol) = await asyncio.gather(
        loop.run_in_executor(None,getbal),
        loop.run_in_executor(None,getitems),
        loop.run_in_executor(None,fetch))
    cur = await loop.run_in_executor(None,partial(getprices,items))
    fdvs = [i["fdv"] for i in items]
    inc = [round(((cur-fdv)/fdv)*100,2) for cur,fdv in zip(cur,fdvs)]
    return jsonify({"items":[items,balance,eth,sol,btc,cur,fdvs,inc]})

@app.route('/api')
def api():
    d = fetch()
    return jsonify(d)

@app.route('/api/historicalbtc')
def histb():
    data = hfetch("BTC")
    return jsonify(data)

@app.route('/api/historicaleth')
def histe():
    data = hfetch("ETH")
    return jsonify(data)

@app.route('/api/historicalsol')
def hists():
    data = hfetch("SOL")
    return jsonify(data)



    

