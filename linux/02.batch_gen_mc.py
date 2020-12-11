import pandas as pd
from jazzstock_bot.common import connector_db as db
import time


def makemc():
    db.insert('''

    INSERT INTO jazzdb.T_STOCK_MC
    SELECT STOCKCODE, DATE, ABS(CLOSE) * SHARE / 100000000000
    FROM jazzdb.T_STOCK_SND_DAY
    JOIN jazzdb.T_DATE_INDEXED A USING (DATE)
    JOIN (SELECT STOCKCODE, DATE, SHARE FROM jazzdb.T_STOCK_SHARES_INFO WHERE HOLDER = '발행주식수') B USING (STOCKCODE,DATE)
    WHERE 1=1
    AND CNT=0


    ''')


makemc()