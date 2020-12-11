import jazzstock_bot.common.connector_db as db
import pandas as pd
from datetime import datetime as dt


def mergeforwin(stockcode, date):
    query = '''

    SELECT STOCKCODE, DATE, WINCODE, SUM 
    FROM jazzdb.T_STOCK_SND_WINDOW_ISOLATED A
    JOIN jazzdb.T_DATE_INDEXED B USING(DATE)
    WHERE A.STOCKCODE = '%s'
    AND DATE = '%s'

    ;
    ''' % (stockcode, date)

    rt = db.selectInclueColumn(query)

    column = rt[1]
    dt = rt[0]

    codeDic = {

        '36': 'MG',
        '45': 'GM',
        '42': 'CS',
        '44': 'MR',
        '35': 'MQ',
        '41': 'CL',
        '43': 'UB',
        '54': 'NM',
        '58': 'DC',
        '61': 'DW',
        '33': 'JP',
        '06': 'SY',
        '03': 'HT',
        '37': 'CT'

    }

    df = pd.DataFrame(data=dt, columns=column)

    ndf = pd.DataFrame()

    for i, each in enumerate(codeDic.keys()):

        if i == 0:
            eachdf = df[df['WINCODE'] == each][['STOCKCODE', 'DATE', 'SUM']].rename(
                columns={"SUM": codeDic[each]}).reset_index(drop=True)
        else:
            eachdf = df[df['WINCODE'] == each][['SUM']].rename(columns={"SUM": codeDic[each]}).reset_index(
                drop=True)

        ndf = pd.concat([ndf, eachdf], axis=1, sort=False)

    ndf['DATE'] = ndf.DATE.apply(str)

    # print(ndf)
    data = str([tuple(l) for l in ndf.values.tolist()])
    insertquery = '''INSERT INTO jazzdb.T_STOCK_SND_WINDOW_MERGED VALUES ''' + str(data)[1:-1]

    db.insert(insertquery)


def db_readAll(dt):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_SND_WINDOW_MERGED
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


itemDic, codeDic = {}, {}

todaydate = dt.now().date()
# todaydate = '2020-12-02'
db_readAll(todaydate)


date_list = [todaydate]
# date_list = ['2020-12-02', '2020-12-03', '2020-12-04']
# date_list = db.selectSingleColumn("SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE DATE > '2020-07-03'")
start = dt.now()


for eachdate in date_list:
    for i, eachCode in enumerate(codeDic.keys()):
        try:
            mergeforwin(eachCode, eachdate)
            print(eachCode)
            if (i % 400 == 0):
                print(i, todaydate, eachCode, dt.now() - start)
        except:
            print('error발생', i, eachCode, codeDic[eachCode])
