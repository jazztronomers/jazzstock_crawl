from jazzstock_bot.common import connector_db as db
import time
import sys
from datetime import datetime as dt


def analysisSndBasicEachDay(stockcode, tdate):
    qa = '''

    SELECT cast(DATE AS CHAR) AS DATE, STOCKCODE, CLOSE, VOLUME, FOREI, INS, PER, FINAN,
       SAMO, YG, TUSIN, INSUR, NATION, BANK, OTHERFINAN,
       OTHERCORPOR, OTHERFOR, CNT, MA3, MA5, MA10, MA20, MA60,
       MA120, VMA3, VMA5, VMA10, VMA20, VMA60, VMA120
    FROM jazzdb.T_STOCK_SND_DAY A
    JOIN jazzdb.T_DATE_INDEXED B USING (DATE)
    JOIN jazzdb.T_STOCK_MA USING (STOCKCODE, DATE)
    WHERE 1=1
    AND A.STOCKCODE = '%s'
    AND B.CNT BETWEEN 56 AND 556

    ''' % (stockcode)

    qb = '''

    SELECT STOCKCODE, SHARE
    FROM jazzdb.T_STOCK_SHARES_INFO
    WHERE 1=1
    AND HOLDER = '유통주식수'
    AND STOCKCODE = '%s'
    AND DATE = '%s'

    ''' % (stockcode, tdate)

    adf = db.selectpd(qa)
    bdf = db.selectpd(qb)



    dic = {

        'INS': 'I',
        'FOREI': 'F',
        'PER': 'PS',
        'FINAN': 'FN',
        'SAMO': 'S',
        'YG': 'YG',
        'TUSIN': 'T',
        'INSUR': 'IS',
        'NATION': 'NT',
        'BANK': 'BK',
        'OTHERCORPOR': 'OC',

    }

    winsize = [1, 3, 5, 20, 60]

    adf.CLOSE = abs(adf.CLOSE)

    # 수급퍼센티지 만들기
    for eachwin in dic.keys():
        for eachsize in winsize:
            adf[dic[eachwin] + str(eachsize)] = adf[eachwin].rolling(eachsize).sum() / bdf.SHARE.values[0]
        # 단일종목역대랭킹뽑기
        if (eachwin in ['YG', 'PER']):
            adf[dic[eachwin][0] + 'R'] = adf[eachwin].rank(method='first', ascending=False)
        else:
            adf[dic[eachwin] + 'R'] = adf[eachwin].rank(method='first', ascending=False)

    # 주가변동퍼센티지
    for eachsize in winsize:
        adf['P' + str(eachsize)] = adf.CLOSE.pct_change(periods=eachsize)

    # 볼륨변동

    # ,ROUND(VOLUME/VMA3,5) AS V3
    # ,ROUND(VOLUME/VMA5,5) AS V5
    # ,ROUND(VOLUME/VMA20,5) AS V20
    # ,ROUND(VOLUME/VMA60,5) AS V60

    for eachsize in winsize[1:]:
        adf['V' + str(eachsize)] = adf.VOLUME / adf['VMA' + str(eachsize)]

    print(adf[['STOCKCODE', 'DATE', 'CLOSE', 'P1', 'P3', 'P5', 'P20', 'P60', 'I1',
                     'I3', 'I5', 'I20', 'I60', 'F1', 'F3', 'F5', 'F20', 'F60', 'PS1', 'PS3',
                     'PS5', 'PS20', 'PS60', 'FN1', 'FN3', 'FN5', 'FN20', 'FN60', 'YG1',
                     'YG3', 'YG5', 'YG20', 'YG60', 'S1', 'S3', 'S5', 'S20', 'S60', 'T1',
                     'T3', 'T5', 'T20', 'T60', 'IS1', 'IS3', 'IS5', 'IS20', 'IS60', 'NT1',
                     'NT3', 'NT5', 'NT20', 'NT60', 'BK1', 'BK3', 'BK5', 'BK20', 'BK60',
                     'OC1', 'OC3', 'OC5', 'OC20', 'OC60', 'IR', 'FR', 'PR', 'FNR', 'YR',
                     'SR', 'TR', 'ISR', 'NTR', 'BKR', 'OCR', 'V3', 'V5', 'V20', 'V60']].tail(1),
                'jazzdb.T_STOCK_SND_ANALYSIS_RESULT_TEMP')

    db.insertdf(adf[['STOCKCODE', 'DATE', 'CLOSE', 'P1', 'P3', 'P5', 'P20', 'P60', 'I1',
                     'I3', 'I5', 'I20', 'I60', 'F1', 'F3', 'F5', 'F20', 'F60', 'PS1', 'PS3',
                     'PS5', 'PS20', 'PS60', 'FN1', 'FN3', 'FN5', 'FN20', 'FN60', 'YG1',
                     'YG3', 'YG5', 'YG20', 'YG60', 'S1', 'S3', 'S5', 'S20', 'S60', 'T1',
                     'T3', 'T5', 'T20', 'T60', 'IS1', 'IS3', 'IS5', 'IS20', 'IS60', 'NT1',
                     'NT3', 'NT5', 'NT20', 'NT60', 'BK1', 'BK3', 'BK5', 'BK20', 'BK60',
                     'OC1', 'OC3', 'OC5', 'OC20', 'OC60', 'IR', 'FR', 'PR', 'FNR', 'YR',
                     'SR', 'TR', 'ISR', 'NTR', 'BKR', 'OCR', 'V3', 'V5', 'V20', 'V60']].tail(1),
                'jazzdb.T_STOCK_SND_ANALYSIS_RESULT_TEMP')


def db_readAll(dt):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_SND_ANALYSIS_RESULT_TEMP
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


itemDic = {}
codeDic = {}

def gettoday():
    td = db.selectSingleValue('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0')
    return td

itemDic, codeDic = {},{}

if len(sys.argv)>1:
    todaydate = sys.argv[1]
else:
    todaydate = gettoday()
    
    
db_readAll(todaydate)


start =datetime.now()
#
for i,eachCode in enumerate(list(codeDic.keys())):

    try:

        st = dt.now()
        analysisSndBasicEachDay(eachCode, todaydate)
        print(i,todaydate,eachCode,dt.now()-start, dt.now()-st)
    except Exception as e:
        print('error',todaydate,eachCode,e)

#
