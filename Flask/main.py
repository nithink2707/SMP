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
JUPITER_API_KEY = 'jup_57fbd7106d63f2dbb6ff92bb12b964142875cbc238698678fdf69631c968bbb9'
base = ['So11111111111111111111111111111111111111111','pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn','So11111111111111111111111111111111111111112']
db_pool = psycopg2.pool.ThreadedConnectionPool(1,45,DBURL)
position_executor = ThreadPoolExecutor(max_workers=20)
POSITION_BATCH_SIZE = 100

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

    for start in range(0, len(positions), POSITION_BATCH_SIZE):
        position_executor.submit(
            check_position, positions[start:start + POSITION_BATCH_SIZE]
        )
    
def close_all_positions():
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT CA, Name, Initial FROM port WHERE SellBal IS NULL"
            )
            open = cursor.fetchall()

    finally:
        return_db(conn)

    for start in range(0, len(open), POSITION_BATCH_SIZE):
        position_executor.submit(
            close_position, open[start:start + POSITION_BATCH_SIZE]
        )

def close_position(positions):
    if not positions:
        return

    try:
        fdvs = retry([position[0] for position in positions])
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                for ca, tick, initial_fdv in positions:
                    final_fdv = fdvs.get(ca)
                    if final_fdv is None:
                        continue

                    profit = 0.1 * (final_fdv - initial_fdv) / initial_fdv
                    cursor.execute(
                        """
                        UPDATE port
                        SET Final = %s, SellBal = COALESCE(SellBal, 0) + %s, pnl = %s
                        WHERE CA = %s AND SellBal IS NULL
                        """,
                        (
                            final_fdv,
                            profit + 0.1,
                            (final_fdv - initial_fdv) / initial_fdv,
                            ca,
                        ),
                    )
                    if cursor.rowcount:
                        cursor.execute(
                            "UPDATE bal SET Balance = Balance + %s",
                            (profit + 0.1,),
                        )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db(conn)
    except (requests.RequestException, ValueError) as error:
        print(f"Price check failed for position batch: {error}")

def retry(mints, max_tries=4):
    if not JUPITER_API_KEY:
        raise RuntimeError("JUPITER_API_KEY environment variable is required")

    pending = list(dict.fromkeys(mints))
    fdvs = {}
    for _ in range(max_tries):
        if not pending:
            break

        response = requests.get(
            "https://api.jup.ag/tokens/v2/search",
            params={"query": ",".join(pending)},
            headers={"x-api-key": JUPITER_API_KEY},
            timeout=10,
        )
        response.raise_for_status()
        tokens = response.json()
        if not isinstance(tokens, list):
            raise ValueError("Unexpected response from Jupiter token search")

        found = {}
        for token in tokens:
            mint = token.get("id")
            fdv = token.get("fdv")
            if mint in pending and fdv is not None:
                found[mint] = fdv

        fdvs.update(found)
        pending = [mint for mint in pending if mint not in found]

    return fdvs


def getprices(items):
    mints = [item["ca"] for item in items]
    fdvs = {}
    for start in range(0, len(mints), 15):
        fdvs.update(retry(mints[start:start + 15]))
    return [fdvs.get(mint) for mint in mints]

def getitems():
    sql = get_db()
    try:
        cursor = sql.cursor()
        cursor.execute("SELECT Name,Initial,CA FROM port WHERE final is NULL")
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

def check_position(positions):
    if not positions:
        return

    try:
        fdvs = retry([position[0] for position in positions])
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                for ca, tick, initial_fdv in positions:
                    final_fdv = fdvs.get(ca)
                    if final_fdv is None or final_fdv < 2 * initial_fdv:
                        continue

                    profit = 0.1 * (final_fdv - initial_fdv) / initial_fdv
                    cursor.execute(
                        """
                        UPDATE port
                        SET Final = %s, SellBal = COALESCE(SellBal, 0) + %s, pnl = %s
                        WHERE CA = %s AND SellBal IS NULL
                        """,
                        (
                            final_fdv,
                            profit + 0.1,
                            (final_fdv - initial_fdv) / initial_fdv,
                            ca,
                        ),
                    )
                    if cursor.rowcount:
                        cursor.execute(
                            "UPDATE bal SET Balance = Balance + %s",
                            (profit + 0.1,),
                        )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db(conn)
    except (requests.RequestException, ValueError) as error:
        print(f"Price check failed for position batch: {error}")
    
        

app = Flask(__name__)
CORS(app)

scheduler = BackgroundScheduler()
scheduler.add_job(
    func=check_all_positions,
    trigger="interval",
    minutes=15,
    max_instances=1,
    coalesce=True,
)
scheduler.add_job(
    func =close_all_positions,
    trigger="interval",
    hours=24,
    max_instances=1,
    coalesce=True
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
    print(transactions)
    for transaction in transactions:
        for transfer in transaction.get('tokenTransfers', []):
            ca = transfer.get('mint')
            if not ca or ca in base or ca in seen_addresses:
                continue
            print("checking",ca)
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
            print(f'data is {data}')
            token = data[0]
            tick = token.get('baseToken', {}).get('symbol')
            fdv = token.get('fdv')
            native_transfers = transaction.get('nativeTransfers', [])
            amount = native_transfers[0].get('amount', 0) / 1e9 if native_transfers else 0
            fee_payer = transaction.get('feePayer')
            print(f'{tick} {fdv} {amount}')

            if not tick or fdv is None or fdv <= 80000 or amount <= 0.025:
                continue

            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT 1 FROM port WHERE CA = %s', (ca,))
                    if cursor.fetchone():
                        continue

                    cursor.execute(
                        """
                        INSERT INTO port (Name, Initial, CA, BuyBal, Followed)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (tick, fdv, ca, -0.1, fee_payer),
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
    inc = [
    None if current is None
    else round(((current - initial) / initial) * 100, 2)
    for current, initial in zip(cur, fdvs)
]
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



    
