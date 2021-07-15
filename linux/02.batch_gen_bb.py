from jazzstock_bot.common import connector_db as db
import pandas as pd
import time
import sys


def db_readAll():
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.LISTED = 1
                                                        """

    for eachRow in db.select(query):
        if (len(eachRow) > 0):
            itemDic[eachRow[1].upper()] = eachRow[0]
            codeDic[eachRow[0]] = eachRow[1].upper()

    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()))


def makebb(stockcode, day):
    df = db.selectpd('''

    SELECT STOCKCODE, cast(DATE AS CHAR) as DATE, CLOSE
    FROM jazzdb.T_STOCK_OHLC_DAY
    JOIN jazzdb.T_DATE_INDEXED USING (DATE)
    WHERE 1=1
    AND STOCKCODE = '%s'
    AND DATE > '2020-08-01'
    # AND CNT<25i

    ''' % (stockcode))

    bbwinsize = 20

    df['PMA20'] = df['CLOSE'].rolling(20).mean()
    df['PBBU'] = df['PMA20'] + 2 * df['CLOSE'].rolling(bbwinsize).std().round(0)
    df['PBBL'] = df['PMA20'] - 2 * df['CLOSE'].rolling(bbwinsize).std().round(0)
    df['BBPOS'] = (df['CLOSE'] - df['PBBL']) / (df['PBBU'] - df['PBBL']).round(3)
    df['BBWIDTH'] = (4 * df['CLOSE'].rolling(bbwinsize).std() / df['PMA20']).round(3)
    data = []

    for each in df[df['DATE'] == day ].dropna()[['DATE', 'PBBU', 'PBBL', 'BBPOS', 'BBWIDTH']].round(3).values.tolist():
        l = each
        l.insert(0, stockcode)
        data.append(tuple(l))


    insertQuery = '''

        INSERT INTO jazzdb.T_STOCK_BB
        VALUES ''' + str(data)[1:-1]


    db.insert(insertQuery)




def gettoday():
    td = db.selectSingleValue('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0')
    return td


    
    
codeDic, itemDic = {}, {}
db_readAll()

if len(sys.argv)> 1:
    today = sys.argv[1]
else:
    today = gettoday()

print(today)

if __name__ == '__main__':

    for i, eachcode in enumerate(codeDic.keys()):
        try:
            makebb(eachcode, today)
            if i%500 == 0:
                print(i, eachcode, 'DONE')
        except Exception as e:
            print(i, eachcode, e)
            
        
        
            # print('error %s' % (each))


