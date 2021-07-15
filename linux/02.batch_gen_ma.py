import sys
from jazzstock_bot.common import connector_db as db
from datetime import datetime as dt


def db_readAll(dt):
    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_MA
                            WHERE DATE = '%s'
                            GROUP BY STOCKCODE
                        )
                        AND A.LISTED = 1
                                                        """ % (dt)

    for eachRow in db.select(query):
        if (len(eachRow) > 0):
            itemDic[eachRow[1].upper()] = eachRow[0]
            codeDic[eachRow[0]] = eachRow[1].upper()

    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()))
    
    
def insert_movingaverage(stockcode, date):
    
    query = '''

        INSERT INTO jazzdb.T_STOCK_MA
        SELECT STOCKCODE, DATE, MA3, MA5, MA10, MA20, MA60, MA120, VMA3, VMA5, VMA10, VMA20, VMA60, VMA120
        FROM
        (
        SELECT STOCKCODE, DATE, B.CNT, CLOSE, VOLUME,

            AVG(ABS(CLOSE)) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MA3,
            AVG(ABS(CLOSE)) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA5,
            AVG(ABS(CLOSE)) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS MA10,
            AVG(ABS(CLOSE)) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS MA20,
            AVG(ABS(CLOSE)) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS MA60,
            AVG(ABS(CLOSE)) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS MA120,

            AVG(VOLUME) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS VMA3,
            AVG(VOLUME) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS VMA5,
            AVG(VOLUME) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS VMA10,
            AVG(VOLUME) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS VMA20,
            AVG(VOLUME) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS VMA60,
            AVG(VOLUME) OVER (PARTITION BY STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS VMA120
        FROM jazzdb.T_STOCK_SND_DAY A
        JOIN jazzdb.T_DATE_INDEXED B USING (DATE)
        WHERE 1=1
        AND B.CNT < 200
        AND A.STOCKCODE = '%s'
        ) RS
        WHERE RS.DATE ='%s'


            ''' %(stockcode, date)
    
    try:
        db.insert(query)
        # print('MA SUCCESS', i,stockcode,codeDic[stockcode])
    except Exception as e:
        print('ERROR', i,stockcode, codeDic[stockcode], e)
    
def gettoday():
    td = db.selectSingleValue('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0')
    return td

itemDic, codeDic = {},{}

if len(sys.argv)>1:
    todaydate = sys.argv[1]
else:
    todaydate = gettoday()
    
    
db_readAll(todaydate)


for i,eachcode in enumerate(codeDic.keys()):
    insert_movingaverage(eachcode, todaydate)