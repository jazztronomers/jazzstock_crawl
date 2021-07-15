from jazzstock_bot.common import connector_db as db
import pandas as pd
import time
import sys


pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False

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
    
    
class StockMovingAverage:
    
    
    def __init__(self, stockcode):
        
        self.stockcode = stockcode
        self.df = self.get_ohlc()
        self.df = self._movingaverage_price(self.df)
        self.df = self._movingaverage_volume(self.df)
        
    def get_ohlc(self, cnt=900):
        
        date_from = db.selectSingleValue('SELECT CAST(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = "%s"'%(cnt))
        df = db.selectpd("SELECT STOCKCODE, CAST(DATE AS CHAR) AS DATE, A.OPEN, A.HIGH, A.LOW, A.CLOSE, VOLUME FROM jazzdb.T_STOCK_OHLC_DAY A JOIN jazzdb.T_STOCK_SND_DAY B USING(STOCKCODE, DATE) WHERE STOCKCODE = '%s' AND DATE > '%s'"%(self.stockcode, date_from))
        return df

    def _movingaverage_price(self, ipdf, n=[5,20,60,120,240], type = 0):
        '''
        :param df: META DATA + OHLC DATAFRAME, 60이평을 얻기 위해서는 최소 120줄은 들어와야 함.
        :param n: 이동평균 단위
        :param type: 0: 입력 그대로 출력하려면, nan 값 주의
                     1: 마지막 한줄만 출력하려면
        :return: row
        '''

        opdf = ipdf.copy()
        for each in n:
            opdf['PSMA'+str(each)]=opdf['CLOSE'].rolling(each).mean()

        for each in n:
            opdf['PSMAR' + str(each)] = (opdf['CLOSE'] - opdf['CLOSE'].rolling(each).mean()) / opdf['CLOSE'].rolling(each).mean()

        return opdf

    def _movingaverage_volume(self, ipdf, n=[5,20,60,120,240], type = 0):
        '''
        :param df: META DATA + OHLC DATAFRAME, 60이평을 얻기 위해서는 최소 120줄은 들어와야 함.
        :param n: 이동평균 단위
        :param type: 0: 입력 그대로 출력하려면, nan 값 주의
                     1: 마지막 한줄만 출력하려면
        :return: row
        '''

        opdf = ipdf.copy()
        for each in n:
            opdf['VSMA'+str(each)]=opdf['VOLUME'].rolling(each).mean()
        for each in n:
            opdf['VSMAR' + str(each)] = (opdf['VOLUME'] - opdf['VOLUME'].rolling(each).mean()) / opdf['VOLUME'].rolling(each).mean()
        return opdf


    def gettoday():
        td = db.selectSingleValue('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0')
        return td
    
    def get_dataframe(self):
        return self.df
    
    
    def insert_to_database(self, n=3):
        db.insertdf(self.df[['STOCKCODE', 'DATE', 'PSMAR5','PSMAR20', 'PSMAR60', 'PSMAR120', 'PSMAR240', 'VSMAR5','VSMAR20','VSMAR60', 'VSMAR120', 'VSMAR240']].round(3).tail(n), table='jazzdb.T_STOCK_DAY_SMAR')

        
if __name__=="__main__":
    codeDic, itemDic = {}, {}
    db_readAll()
    for i, stockcode in enumerate(codeDic.keys()): 
        StockMovingAverage(stockcode).insert_to_database(n=1)
        # print(i, stockcode)