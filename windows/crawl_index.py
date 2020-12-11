from jazzstock_crawl.windows.kiwoom import dateManager as dp
from jazzstock_bot.common import connector_db as db
from jazzstock_crawl.windows.kiwoom import Kiwoom as kapi, dateManager as dm, apiManager as am
from jazzstock_crawl.wrapper import _check_running_time
from jazzstock_bot.util import index_calculator as cal
import time
apiObj = kapi.Kiwoom()
apiObj.comm_connect()


recent_trading_day, _ = am.api_checkDate(apiObj, dp.todayStr('n'))
codeDic, itemDic = {}, {}

def db_readAll():
    q = '''
    SELECT INDEXCODE, INDEXNAME
    FROM jazzdb.T_INDEX_CODE_MGMT
    WHERE 1=1    
    '''
    df = db.selectpd(q)
    for each in df.values:
        codeDic[each[0]]=each[1]

db_readAll()

def sync_ohlc_day(code, dt=recent_trading_day):
    '''

    특정지수의 일봉을 업데이트하는 함수

    :param code:
    :param dt:
    :return:
    '''

    # 일단 최신데이터를 키움증권에서 땡겨옴
    RESPONSE_WHOLE = am.api_index_ohlc_day(apiObj, code, dt)


    # OHLC테이블에 존재하는 테이블
    EXIST_DATE_LIST = getalreadyexists(code, 'jazzdb.T_INDEX_OHLC_DAY')


    if(len(EXIST_DATE_LIST)==0):
        RESPONSE_NOT_EXISTS = RESPONSE_WHOLE
    else:
        RESPONSE_NOT_EXISTS =  RESPONSE_WHOLE[(~RESPONSE_WHOLE.DATE.isin(EXIST_DATE_LIST)) & (RESPONSE_WHOLE.DATE > EXIST_DATE_LIST.DATE.max())]

    # 새로운 데이터가 있다면 INSERT
    if(len(RESPONSE_NOT_EXISTS)>0):
        RESPONSE_NOT_EXISTS['INDEXCODE'] = code

        print(code, RESPONSE_NOT_EXISTS[['INDEXCODE','DATE','OPEN','HIGH','LOW','CLOSE','VOLUME','VALUE']])

        db.insertdf(RESPONSE_NOT_EXISTS[['INDEXCODE','DATE','OPEN','HIGH','LOW','CLOSE','VOLUME','VALUE']],'jazzdb.T_INDEX_OHLC_DAY')
    else:

        print("DONE ALREADY")



def makebb(code):

    q='''
        SELECT REPLACE(DATE, '-', '') AS DATE, CLOSE  
        FROM jazzdb.T_INDEX_OHLC_DAY 
        WHERE 1=1
        AND INDEXCODE = '%s'
      '''%(code)

    df = db.selectpd(q)
    BB_WHOLE = cal._bolinger(df)
    EXIST_DATE_LIST = getalreadyexists(code, 'jazzdb.T_INDEX_BB')

    if(len(EXIST_DATE_LIST)==0):
        BB_NOT_EXISTS = BB_WHOLE
    else:
        BB_NOT_EXISTS =  BB_WHOLE[(~BB_WHOLE.DATE.isin(EXIST_DATE_LIST)) & (BB_WHOLE.DATE > EXIST_DATE_LIST.DATE.max())]



    if (len(BB_NOT_EXISTS) > 0):
        BB_NOT_EXISTS['INDEXCODE'] = code
        db.insertdf(BB_NOT_EXISTS[['INDEXCODE','DATE','BBU','BBL','BBP','BBW']],'jazzdb.T_INDEX_BB')
    else:
        print("DONE ALREADY")



def getalreadyexists(code ,table):
    '''
    :param code: 해당종목이
    :param table: 해당테이블에 존재하지 않는 날짜들을
    :return: RETURN 받는다!
    '''

    q = '''
    SELECT REPLACE(DATE, '-', '') AS DATE
    FROM
    (
        SELECT DATE, ROW_NUMBER() OVER (PARTITION BY INDEXCODE ORDER BY DATE DESC) AS RN 
        FROM %s
        WHERE 1=1
        AND INDEXCODE='%s'
        ORDER BY DATE DESC
    ) RS
    WHERE 1=1
    AND RN < 40
    ;
    '''%(table, code)




    return db.selectpd(q)

for eachcode, eachname in db.selectpd('SELECT INDEXCODE, INDEXNAME FROM jazzdb.T_INDEX_CODE_MGMT').values:

    print(eachcode, eachname)
    sync_ohlc_day(eachcode)
    makebb(eachcode)
    time.sleep(0.7)










# *OPT20003 : 전업종 지수요청 001 / 101/

# OPT20002 : 업종별 주가요청
# OPT20005 : 업종별 분봉조회
# OPT20006 : 업종별 일봉조회

