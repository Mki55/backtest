import mysql.connector
from itertools import cycle
import dask.dataframe as dd

# Constants
CEILING = 1700
FIRST_TERM = 1
N = 1700
LAST_TERM = FIRST_TERM + (N - 1) * 1
TOTAL_CAPITAL = N / 2 * (FIRST_TERM + LAST_TERM)

# Initialize current cash and asset values
current_cash = TOTAL_CAPITAL
current_asset = 0
cash_acc = 0


def price_data():
    df = dd.concat([dd.read_parquet('XAUUSD_{}.parquet'.format(i + 1)) for i in range(10)])
    pdf = df.compute()
    values = pdf['Open'].tolist()
    return values

def cf_check(price):
    global current_cash, current_asset, cash_acc
    cash_watermark = TOTAL_CAPITAL - (CEILING - price) * price
    cashflow = current_cash - cash_watermark - cash_acc
    if cashflow > 0:
        cash_acc += cashflow
        return cashflow
    else:
        return 0

def delta_nav(ini_price, price):
    global current_cash, current_asset, cash_acc
    per_nav = round((current_cash + cash_acc + current_asset * price) / TOTAL_CAPITAL * 100, 2)
    per_price = round(price / ini_price * 100, 2)
    return per_nav, per_price

def sell_order(price):
    global current_cash, current_asset
    size = current_asset - (CEILING - price)
    if current_asset >= size:
        current_asset -= size
        current_cash += size * price
    elif current_asset < size:
        size = current_asset
        current_asset -= size
        current_cash += size * price

def buy_order(price):
    global current_cash, current_asset
    size = abs(current_asset - (CEILING - price))
    if current_cash >= size * price:
        current_cash -= size * price
        current_asset += size
    elif size > current_cash:
        size = current_cash / price
        current_asset += size
        current_cash -= size * price


rows = []

def record(ini_price, price,conn,cursor,i):
    global current_cash, current_asset, cash_acc

    if i ==0:
        cf = 0
        cash_acc, per_price, per_nav = 0,100,100
        rows.append((cf, float(cash_acc), float(per_price), float(per_nav)))

    elif i == 10654139:
        insert_query = "INSERT INTO fibo (Cash_Flow, Accumulated_Cash_Flow, Price_Percentage, NAV_Percentage) VALUES (%s, %s, %s, %s)"


        cursor.executemany(insert_query, rows)

        conn.commit()
    else:
        cf = cf_check(price)
        per_nav, per_price = delta_nav(ini_price, price)

        rows.append((cf, cash_acc, per_price, per_nav))

def record_fail(conn,cursor,i):
    global current_cash, current_asset, cash_acc

    if i ==0:
        cf = 0
        cash_acc, per_price, per_nav = 0,100,100
        rows.append((cf, float(cash_acc), float(per_price), float(per_nav)))

    elif i == 10654139:

        # Construct the INSERT statement
        insert_query = "INSERT INTO fibo (Cash_Flow, Accumulated_Cash_Flow, Price_Percentage, NAV_Percentage) VALUES (%s, %s, %s, %s)"

        # Execute the INSERT statement with the rows as a parameter
        cursor.executemany(insert_query, rows)

        # Commit the transaction
        conn.commit()
    else:
        cf = 0
        cash_acc, per_price, per_nav = rows[-1][1:]
        rows.append((cf, float(cash_acc), float(per_price), float(per_nav)))


def is_fibonacci(n):
    if n == 0 or n == 1:
        return True
    a, b = 0, 1
    while b < n:
        a, b = b, a + b
    return b == n

price = price_data()
pre_len = len(price)
ini_price = price[0]

def main():
    conn = mysql.connector.connect(user='root', password='1594', host='localhost', database='backtest')
    cursor = conn.cursor()
    print(pre_len)
    for i in range(pre_len):

        if i == 0:
            cf = 0
            cash_acc, per_price, per_nav = 0, 100, 100
            rows.append((cf, float(cash_acc), float(per_price), float(per_nav)))

        next_trigger = is_fibonacci(i)

        if next_trigger != True:
            record_fail(conn, cursor, i)
            continue

        if not (300 <= price[i] <= 1700):
            record_fail(conn,cursor,i)
            continue

        fixed = CEILING - price[i]
        if current_asset - 0.01 > fixed:
            sell_order(price[i])
            record(ini_price, price[i],conn,cursor,i)
        elif current_asset + 0.01 < fixed:
            buy_order(price[i])
            record(ini_price, price[i],conn,cursor,i)
        else:
            record_fail(conn,cursor,i)


    cursor.close()
    conn.close()
if __name__ == '__main__':
    main()