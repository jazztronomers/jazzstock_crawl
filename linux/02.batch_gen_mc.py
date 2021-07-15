import pandas as pd
import sys
from jazzstock_bot.common import connector_db as db
import time


def makemc(date):
    db.insert('''

    INSERT INTO jazzdb.T_STOCK_MC
    SELECT STOCKCODE, DATE, ABS(CLOSE) * SHARE / 100000000000
    FROM jazzdb.T_STOCK_SND_DAY
    JOIN jazzdb.T_DATE_INDEXED A USING (DATE)
    JOIN (SELECT STOCKCODE, DATE, SHARE FROM jazzdb.T_STOCK_SHARES_INFO WHERE HOLDER = '발행주식수') B USING (STOCKCODE,DATE)
    WHERE 1=1
    AND DATE = '%s'


    '''%(date))


def gettoday():
    td = db.selectSingleValue('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0')
    return td

itemDic, codeDic = {},{}

if len(sys.argv)>1:
    todaydate = sys.argv[1]
else:
    todaydate = gettoday()
    
    
makemc(todaydate)