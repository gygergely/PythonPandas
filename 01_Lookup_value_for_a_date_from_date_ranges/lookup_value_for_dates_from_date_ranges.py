import pandas as pd
import sqlite3

# --- LOAD THE SOURCE DATA SETS TO DATA FRAMES

# get list prices data from github
df_list_prices = pd.read_csv(
    'https://raw.githubusercontent.com/gygergely/PythonPandas/master/00_src_files/list_prices.csv',
    parse_dates=['valid_from', 'valid_to'],
    dtype={'product_id': 'str',
           'list_price': 'float64'})

# get transactions data from github
df_trans = pd.read_csv(
    'https://raw.githubusercontent.com/gygergely/PythonPandas/master/00_src_files/transactions.csv',
    parse_dates=['sales_date'],
    dtype={'sold_qty': 'int64',
           'sales_price': 'float64'})

# --- QUICK LOOK AT THE DATA TYPES OF THE DATA FRAMES
print(df_list_prices.info())
print(df_trans.info())

# --- CREATE AN SQLITE DATABASE READ THE DATA AND LOOK-UP THE LIST PRICES
print(df_trans.shape)

# create an sqlite db in memory
conn = sqlite3.connect(':memory:')

# read the data to db tables
df_trans.to_sql('trans', conn, index=False)
df_list_prices.to_sql('list_prices', conn, index=False)

# sql query to run
sql_query = '''
    SELECT 
        trans.*,
        list_prices.list_price
    FROM
        trans
        LEFT JOIN list_prices 
        ON trans.product_id = list_prices.product_id
        AND (list_prices.valid_from <= trans.sales_date 
            AND list_prices.valid_to >= trans.sales_date)
'''

# re-read the sql query results to df_trans data frame
df_trans = pd.read_sql_query(sql_query, conn)
print(df_trans.shape)

# Print the first 10 items from transactions data frame
print(df_trans.head(10))


