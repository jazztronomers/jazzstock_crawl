import jazzstock_bot.common.connector_db as db
import pandas as pd
import time
pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False


class StockForRecrawledData:
    '''
    OHLC DATAFRAME을 근거하여 모든 META DATA를 생성하는 객체
    STOCKCODE 단위로 모결과를 생성하기에 기존 테이블 단위의 지표생성보다 재사용성이 높음
    수정주가 발생한 종목에 대해서 UPDATE 하는 기능도 구현됨

    목표는 모든 Linx batch python script 를 대체하여
    RAW 데이터만으로 배치데이터를 N개 인스턴스로 분할작업 할 수 있도록 개선


    TO_DO

    batch_gen_ccr
    batch_gen_mc    => OHLC 변경시 T_STOCK_SHARES_INFO까지 조작해줘야함, 3거래일뒤에 발행주식수가 변경되는것 확인함
    batch_gen_snd_analysis_shortterm => 이것 또한..
    batch_gen_snd_analysis_longterm => 이것 또한..

    # =============================================
    profit....
    batch_gen_bb_event
    # =============================================




    DONE

    batch_gen_bb
    batch_gen_ma
    batch_gen_smar
    batch_gen_future_price

    delete and update


    '''



    def __init__(self, stockcode, from_date, to_date):

        self.stockcode = stockcode
        self.from_date = from_date
        self.to_date = to_date
        self.df_ohlc_day_origin = self.get_ohlc_origin()
        self.df_ohlc_day = self.get_ohlc_modified()

        self.merged = self.df_ohlc_day.merge(self.df_ohlc_day_origin, on=['STOCKCODE','DATE'], how='inner')
        self.un_equal_count = (self.merged['CLOSE_x']==self.merged['CLOSE_y']).value_counts().tolist()
        self.equal_ratio = self.un_equal_count[0] / sum(self.un_equal_count)


        # self.equal_ratio = self.un_equal_count[0] / sum(self.un_equal_count)
        # self.generate_all()
        # self.update_all()

    def run(self):
        self.generate_all()
        self.update_all()

    def generate_all(self):
        self.df_bb = self.gen_bb()
        self.df_ma = self.gen_ma()
        self.df_smar = self.gen_smar()
        self.df_profit = self.gen_profit()
        self.df_future = self.gen_future_price()


    def update_all(self):

        self.update_ohlc()
        self.update_bb()
        self.update_ma()
        self.update_smar()
        self.update_future()
        self.update_profit()

    def check_all_columns_in_dataframe(self, df, columns):
        for col in columns:
            if col not in df.columns:
                # print('column "%s" not in dataframe'%(col))
                return False

        return True


    def get_columns_from_table(self, table):
        columns = db.selectpd("SELECT * FROM %s LIMIT 1"%(table)).columns.tolist()
        return columns



    def update_ohlc(self):



        df = self.df_ohlc_day.copy()
        from_date = df.DATE.min()
        to_date = df.DATE.max()
        columns =['STOCKCODE',
                     'DATE',
                     'OPEN',
                     'HIGH',
                     'LOW',
                     'CLOSE',
                     'VALUE',
                     'ADJCLASS',
                     'ADJRATIO',
                     'VOLUME']
        query ="DELETE FROM jazzdb.T_STOCK_OHLC_DAY WHERE STOCKCODE='%s' AND DATE BETWEEN '%s' AND '%s'"%(self.stockcode, from_date, to_date)
        db.delete(query)
        db.insertdf(df[columns], 'jazzdb.T_STOCK_OHLC_DAY')




    def update_bb(self):

        df = self.df_bb.copy()
        table = 'jazzdb.T_STOCK_BB'
        columns = self.get_columns_from_table(table)

        if self.check_all_columns_in_dataframe(df, columns):

            # print("ALL COLUMNS EXISTS")

            df_no_na = df.dropna()
            from_date = df_no_na.DATE.min()
            to_date = df_no_na.DATE.max()
            query = "DELETE FROM jazzdb.T_STOCK_BB WHERE STOCKCODE='%s' AND DATE BETWEEN '%s' AND '%s'" % (self.stockcode, from_date, to_date)
            db.delete(query)
            db.insertdf(df_no_na, 'jazzdb.T_STOCK_BB')





    def update_ma(self):


        df = self.df_ma.copy()
        table = 'jazzdb.T_STOCK_MA'
        columns = self.get_columns_from_table(table)

        # print(df)

        if self.check_all_columns_in_dataframe(df, columns):

            # print("ALL COLUMNS EXISTS")
            df_no_na = df.fillna(0)
            from_date = df_no_na.DATE.min()
            to_date = df_no_na.DATE.max()
            query = "DELETE FROM jazzdb.T_STOCK_MA WHERE STOCKCODE='%s' AND DATE BETWEEN '%s' AND '%s'" % (self.stockcode, from_date, to_date)

            db.delete(query)
            db.insertdf(df_no_na, 'jazzdb.T_STOCK_MA')

            # print(self.stockcode, 'UPDATE_MA DONE')

    def update_smar(self):

        # print(self.stockcode, 'UPDATE_SMAR')
        df_no_na = self.df_smar.dropna()
        from_date = df_no_na.DATE.min()
        to_date = df_no_na.DATE.max()
        query = "DELETE FROM jazzdb.T_STOCK_DAY_SMAR WHERE STOCKCODE='%s' AND DATE BETWEEN '%s' AND '%s'" % (self.stockcode, from_date, to_date)

        db.delete(query)
        db.insertdf(df_no_na, 'jazzdb.T_STOCK_DAY_SMAR')

        # print(self.stockcode, 'UPDATE_SMAR DONE')


    def update_future(self):


        # print(self.stockcode, 'UPDATE_FUTURE')
        df_no_na = self.df_future.dropna()
        from_date = df_no_na.DATE.min()
        to_date = df_no_na.DATE.max()
        query = "DELETE FROM jazzdb.T_STOCK_FUTURE_PRICE WHERE STOCKCODE='%s' AND DATE BETWEEN '%s' AND '%s'" % (
        self.stockcode, from_date, to_date)


        db.delete(query)
        db.insertdf(df_no_na, 'jazzdb.T_STOCK_FUTURE_PRICE')

        # print(self.stockcode, 'UPDATE_FUTURE DONE')


    def update_profit(self):

        df_no_na = self.df_profit.copy()
        from_date = df_no_na.DATE.min()
        to_date = df_no_na.DATE.max()


        for i, row in df_no_na.iterrows():


            # print(row.P60)

            query = '''
            UPDATE `jazzdb`.`T_STOCK_SND_ANALYSIS_RESULT_TEMP` 
            SET `P1` = %s, `P3` = %s, `P5` = %s, `P20` = %s, `P60` = %s 
            WHERE (`STOCKCODE` = '%s') and (`DATE` = '%s');            
            '''%(row.P1, row.P3, row.P5, row.P20, row.P60, self.stockcode, row.DATE)
            query = query.replace('nan', 'NULL')

            # print(query)

            db.insert(query)

            query = '''
            UPDATE `jazzdb`.`T_STOCK_SND_ANALYSIS_LONGTERM` 
            SET `P80` = %s, `P120` = %s, `P160` = %s, `P200` = %s, `P240` = %s 
            WHERE (`STOCKCODE` = '%s') and (`DATE` = '%s');           
            ''' % (row.P80, row.P120, row.P160, row.P200, row.P240,
                   self.stockcode, row.DATE)
            query = query.replace('nan', 'NULL')

            # print(query)
            db.insert(query)

        # print(df_no_na)

        # query = "DELETE FROM jazzdb.T_STOCK_FUTURE_PRICE WHERE STOCKCODE='%s' AND DATE BETWEEN '%s' AND '%s'" % (
        # self.stockcode, from_date, to_date)
        # db.delete(query)
        # db.insert(df_no_na, 'jazzdb.T_STOCK_FUTURE_PRICE')


    def get_ohlc_origin(self):

        query = '''

        SELECT STOCKCODE, CAST(DATE AS CHAR) AS DATE, OPEN, HIGH, LOW, CLOSE
        FROM jazzdb.T_STOCK_OHLC_DAY
        JOIN jazzdb.T_DATE_INDEXED USING (DATE)
        WHERE 1=1
        AND STOCKCODE = "%s"
        AND DATE BETWEEN "%s" AND "%s"

        ''' % (self.stockcode, self.from_date, self.to_date)

        return db.selectpd(query)

    def get_ohlc_modified(self):


        query = '''
        
        SELECT STOCKCODE, CAST(DATE AS CHAR) AS DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, VALUE, 0 AS ADJCLASS, -1 AS ADJRATIO
        FROM jazzdb.T_STOCK_OHLC_DAY_CORRECTION
        JOIN jazzdb.T_DATE_INDEXED USING (DATE)
        WHERE 1=1
        AND STOCKCODE = "%s"
        AND DATE BETWEEN "%s" AND "%s"
        
        '''%(self.stockcode, self.from_date, self.to_date)

        return db.selectpd(query)


    def gen_bb(self):
        df = self.df_ohlc_day.copy()
        bbwinsize = 20

        df['PMA20'] = df['CLOSE'].rolling(20).mean()
        df['BBU'] = df['PMA20'] + 2 * df['CLOSE'].rolling(bbwinsize).std().round(0)
        df['BBL'] = df['PMA20'] - 2 * df['CLOSE'].rolling(bbwinsize).std().round(0)
        df['BBP'] = (df['CLOSE'] - df['BBL']) / (df['BBU'] - df['BBL']).round(3)
        df['BBW'] = (4 * df['CLOSE'].rolling(bbwinsize).std() / df['PMA20']).round(3)

        return df[['STOCKCODE', 'DATE', 'BBU', 'BBL','BBP','BBW']]


    def gen_ma(self, intervals = [3,5,10,20,60,120], meta=False):
        '''

        WEB에서 MOVING AVERAGE 선 그릴때 그냥 가져다 씀

        '''


        col_list = []
        df = self.df_ohlc_day.copy()
        for ma in ['MA', 'VMA']:
            for interval in intervals:

                col_list.append(ma+str(interval))
                df[ma+str(interval)] = df['CLOSE'].rolling(interval).mean()

        if meta:
            return df[['STOCKCODE', 'DATE', 'CLOSE']+col_list]
        else:
            return df[['STOCKCODE', 'DATE']+col_list]

    def gen_smar(self, intervals=[5, 20, 60, 120, 240]):

        columns= ['STOCKCODE',
         'DATE',
         'PSMAR5',
         'PSMAR20',
         'PSMAR60',
         'PSMAR120',
         'PSMAR240',
         'VSMAR5',
         'VSMAR20',
         'VSMAR60',
         'VSMAR120',
         'VSMAR240']

        df = self.gen_ma(intervals, meta=True)
        col_list = []
        for smar in ['PSMAR', 'VSMAR']:
            for interval in intervals:
                ma = 'MA' if smar == 'PSMAR' else 'VMA'

                col_list.append(smar+str(interval))
                df[smar + str(interval)] = (df.CLOSE - df[ma+str(interval)]) / df.CLOSE



        return df[columns]




    def gen_profit(self, intervals=[1, 3, 5, 20, 60, 80, 120, 160, 200, 240]):

        df = self.df_ohlc_day.copy()
        col_list = []
        for interval in intervals:
            col_list.append("P"+str(interval))
            df['P' + str(interval)] = df.CLOSE.pct_change(periods=interval)

        return df[['STOCKCODE', 'DATE', 'CLOSE'] + col_list]


    def gen_future_price(self, intervals=[1,3,5,10,20,60]):

        '''
        PRA PRO PRH PRL
        '''


        columns=['STOCKCODE',
         'DATE',
         'PRA1',
         'PRA3',
         'PRA5',
         'PRA10',
         'PRA20',
         'PRA60',
         'PRO1',
         'PRO3',
         'PRO5',
         'PRO10',
         'PRO20',
         'PRO60',
         'PRH1',
         'PRH3',
         'PRH5',
         'PRH10',
         'PRH20',
         'PRH60',
         'PRL1',
         'PRL3',
         'PRL5',
         'PRL10',
         'PRL20',
         'PRL60']


        df = self.df_ohlc_day.copy().sort_values(by="DATE", ascending=False)

        df['SHIFT_HIGH_BY_1'] = df['HIGH'].shift(1)
        df['SHIFT_LOW_BY_1'] = df['LOW'].shift(1)
        df['SHIFT_OPEN_BY_1'] = df['OPEN'].shift(1)

        for interval in intervals:
            df['SHIFT_CLOSE' + str(interval)] = df['CLOSE'].shift(interval)

        for interval in intervals:
            df['PRA' + str(interval)] = (df['SHIFT_CLOSE' + str(interval)] - df['CLOSE'] ) / df['CLOSE']
            df['PRO' + str(interval)] = (df['SHIFT_CLOSE' + str(interval)] - df['SHIFT_OPEN_BY_1']) / df['SHIFT_OPEN_BY_1']
            df['PRH' + str(interval)] = (df['SHIFT_HIGH_BY_1'].rolling(interval, min_periods=1).max() - df['SHIFT_OPEN_BY_1']) / df['SHIFT_OPEN_BY_1']
            df['PRL' + str(interval)] = (df['SHIFT_LOW_BY_1'].rolling(interval, min_periods=1).min() - df['SHIFT_OPEN_BY_1']) / df['SHIFT_OPEN_BY_1']

        return df.sort_values(by="DATE", ascending=True)[columns]


def get_all_stockcodes():
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """
            SELECT DISTINCT(STOCKCODE) AS STOCKCODE
            FROM jazzdb.T_STOCK_OHLC_DAY_CORRECTION
            JOIN jazzdb.T_STOCK_CODE_MGMT USING (STOCKCODE)
            WHERE 1=1
            AND LISTED = '1'
            AND DATE = '2021-09-24'

            """

    return db.selectSingleColumn(query)


if __name__=="__main__":

    # stockcode = '580023'
    # obj = StockForRecrawledData(stockcode=stockcode, from_date='2016-09-23', to_date="2021-09-24")
    # if obj.equal_ratio<0.99:
    #     print(obj.equal_ratio)
    #     obj.run()


    today = '2021-09-24'


    mn, mx = 2124, 2500


    for i, stockcode in enumerate(get_all_stockcodes()):


        if i >= mn and i < mx:
            obj = StockForRecrawledData(stockcode=stockcode, from_date='2016-09-23', to_date="2021-09-24")
            if obj.equal_ratio < 0.996:
                print(stockcode, i, obj.equal_ratio, mn, mx)
                try:
                    obj.run()
                except Exception as e:
                    print(e)
        # else:
        #     print(i, stockcode, obj.equal_ratio, 'corrected...')



