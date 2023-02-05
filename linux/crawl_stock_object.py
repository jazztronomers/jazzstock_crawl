from jazzstock_bot.common import connector_db as db
from datetime import datetime
import pandas as pd
import time
import sys


pd.options.display.expand_frame_repr=False
pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500


INTERVAL_RANGE = 120 #
INTERVAL_ROLLING = 20


def get_all_stocks():
    '''
    DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')
    '''

    query = """

        SELECT A.STOCKCODE, A.STOCKNAME
        FROM jazzdb.T_STOCK_CODE_MGMT A
        WHERE 1=1
        AND A.LISTED = 1

    """
    stocks = db.selectpd(query).to_dict(orient="records")
    info("stockcount: %s" % (len(stocks)))
    return stocks

def get_date_from_to():
    date_list = db.selectSingleColumn('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT < %s'%(INTERVAL_RANGE))
    today = date_list[-1]
    from_date = date_list[0]
    return today, from_date, date_list

def info(message):
    print("info | %s | %s"%(datetime.now(), message))

def warn(message):
    print("warn | %s | %s" % (datetime.now(), message))

def alert(message):
    print("info | %s | %s" % (datetime.now(), message))

def check_running_time(f):
    def wrapper(*args):
        start = datetime.now()

        try:
            ret = f(*args)
            elapsed_time = datetime.now() - start
            info("%s, %s"%(f.__name__, elapsed_time))

            if not isinstance(ret, pd.DataFrame):
                raise Exception("result should be a dataframe")


            return ret

        except Exception as e:
            warn("%s, %s, %s" % (f.__name__, elapsed_time, e))
            return False

    return wrapper

class CrawlStockObject:

    def __init__(self, stockcode, from_date, to_date):

        self.stockcode = stockcode
        self.from_date = from_date
        self.to_date = to_date
        self.today = to_date

        self.table_bb = "jazzdb.T_STOCK_BB"
        self.table_bb_event = "jazzdb.T_STOCK_BB_EVENT"
        self.table_future_price = "jazzdb.T_STOCK_FUTURE_PRICE"            # QUERY
        self.table_moving_average = "jazzdb.T_STOCK_MA"
        self.table_smar = "jazzdb.T_STOCK_DAY_SMAR"

        self.table_snd_shorterm = "jazzdb.T_STOCK_SND_ANALYSIS_RESULT_TEMP"
        self.table_snd_longterm = "jazzdb.T_STOCK_SND_ANALYSIS_LONGTERM"
        self.table_vwap = "jazzdb.T_STOCK_VWAP"

        self.dev = True

        # self.table_mc = "jazzdb.T_STOCK_MC" # BATCHING BY QUERY
        # self.table_ccr = "jazzdb.T_STOCK_CCR" # BATCHING BY QUERY

    @check_running_time
    def main(self):

        # GET DATA
        self.df_bb = self.make_bb()
        self.df_bb_event = self.make_bb_event()
        self.df_future_price = self.make_future_price()
        self.df_moving_average = self.make_moving_average()
        self.df_smar = self.make_smar()
        self.df_snd_shorterm = self.make_snd_shortterm()
        self.df_snd_longterm_to_be = self.make_snd_longterm_to_be()
        self.df_vwap = self.make_vwap()

        # INSERT

        self.check_and_insert(self.df_bb, self.table_bb)
        self.check_and_insert(self.df_bb_event, self.table_bb_event)
        self.check_and_insert(self.df_future_price, self.table_future_price)
        self.check_and_insert(self.df_moving_average, self.table_moving_average)
        self.check_and_insert(self.df_smar, self.table_smar)
        self.check_and_insert(self.df_snd_shorterm, self.table_snd_shorterm)
        self.check_and_insert(self.df_snd_longterm_to_be, self.table_snd_longterm)
        # self.check_and_insert(self.df_vwap, self.table_vwap)



    def check_and_insert(self, dataframe, target_table, row=1):

        query_to_get_column = """
        
        SELECT *
        FROM %s
        LIMIT 1
        
        """%(target_table)


        try:
            columns = db.selectpd(query_to_get_column).columns

            cnt = 0
            for col in columns:
                if col not in dataframe.columns:
                    cnt +=1
                    warn("shit %s not in dataframe...%s"%(col, target_table))

            if cnt == 0:
                info('%s, %s' % (target_table, "validated"))
        except Exception as e:


            warn(e)


    @check_running_time
    def make_bb(self):
        df = db.selectpd('''

        SELECT STOCKCODE, cast(DATE AS CHAR) as DATE, CLOSE
        FROM jazzdb.T_STOCK_OHLC_DAY
        JOIN jazzdb.T_DATE_INDEXED USING (DATE)
        WHERE 1=1
        AND STOCKCODE = '%s'
        AND DATE > '2020-08-01'
        # AND CNT<25i

        ''' % (self.stockcode))

        bbwinsize = 20

        df['PMA20'] = df['CLOSE'].rolling(20).mean()
        df['BBU'] = df['PMA20'] + 2 * df['CLOSE'].rolling(bbwinsize).std().round(0)
        df['BBL'] = df['PMA20'] - 2 * df['CLOSE'].rolling(bbwinsize).std().round(0)
        df['BBP'] = (df['CLOSE'] - df['BBL']) / (df['BBU'] - df['BBL']).round(3)
        df['BBW'] = (4 * df['CLOSE'].rolling(bbwinsize).std() / df['PMA20']).round(3)

        return df[["STOCKCODE", "DATE", "BBU", "BBL", "BBP", "BBW"]]

    @check_running_time
    def make_bb_event(self, threshold=0, mid =0.5):

        bbp = self.df_bb.BBP.values
        bbw = self.df_bb.BBW.values
        date = self.df_bb.DATE.values

        prev_point = 0
        prev_event = '-'
        curr_event = None

        ret_df = pd.DataFrame(columns=['PREV_IDX', 'DATE_IDX',
                                       'DATE_FROM', 'DATE_TO', 'INTERVAL',
                                       'PREV_EVENT', 'CURR_EVENT', 'BBP', 'BBW'])

        for i in range(0, len(bbp) - 1):

            # BBP 상향돌파
            if bbp[i + 1] > bbp[i]:
                if bbp[i] < 1 - threshold <= bbp[i + 1]:
                    curr_event = 'UU'
                elif bbp[i] < mid <= bbp[i + 1]:
                    curr_event = 'MU'
                elif bbp[i] < threshold <= bbp[i + 1]:
                    curr_event = 'LU'


            # BBP 하향돌파
            elif bbp[i + 1] < bbp[i]:

                if bbp[i] > threshold >= bbp[i + 1]:
                    curr_event = 'LD'
                elif bbp[i] > mid >= bbp[i + 1]:
                    curr_event = 'MD'
                elif bbp[i] > 1 - threshold >= bbp[i + 1]:
                    curr_event = 'UD'

            if curr_event is not None and curr_event != prev_event:
                ret_df.loc[len(ret_df)] = [prev_point, i + 1,
                                           date[prev_point], date[i + 1], i + 1 - prev_point,
                                           prev_event, curr_event,
                                           bbp[i + 1], bbw[i + 1]]
                prev_event = curr_event
                prev_point = i + 1

            elif i + 1 == len(bbp) - 1:
                ret_df.loc[len(ret_df)] = [prev_point, i + 1,
                                           date[prev_point], date[i + 1], i + 1 - prev_point,
                                           prev_event, '--',
                                           bbp[i + 1], bbw[i + 1]]

        result_df = pd.DataFrame()

        prev_event_list = ret_df.PREV_EVENT.values.tolist()
        curr_event_list = ret_df.CURR_EVENT.values.tolist()

        interval_list = ret_df.PREV_IDX.values.tolist()
        bbw_list = ret_df.BBW.values.tolist()
        bbp_list = ret_df.BBP.values.tolist()

        if curr_event_list[-1] != '--':
            prev_event_list.append(curr_event_list[-1])
            interval_list.append(59)

        result_df.loc[0, 'STOCKCODE'] = self.stockcode
        result_df.loc[0, 'DATE'] = self.today

        result_df.loc[0, 'L1BE'] = '%s' % (prev_event_list[-1])
        result_df.loc[0, 'L2BE'] = '%s' % (prev_event_list[-2])
        result_df.loc[0, 'L3BE'] = '%s' % (prev_event_list[-3])
        result_df.loc[0, 'L4BE'] = '%s' % (prev_event_list[-4])

        result_df.loc[0, 'L1ED'] = int(59 - interval_list[-1])
        result_df.loc[0, 'L2ED'] = int(59 - interval_list[-2])
        result_df.loc[0, 'L3ED'] = int(59 - interval_list[-3])
        result_df.loc[0, 'L4ED'] = int(59 - interval_list[-4])

        result_df.loc[0, 'L1BW'] = round(bbw_list[-1], 3)
        result_df.loc[0, 'L2BW'] = round(bbw_list[-2], 3)
        result_df.loc[0, 'L3BW'] = round(bbw_list[-3], 3)
        result_df.loc[0, 'L4BW'] = round(bbw_list[-4], 3)

        result_df.loc[0, 'L1BP'] = round(bbp_list[-1], 3)
        result_df.loc[0, 'L2BP'] = round(bbp_list[-2], 3)
        result_df.loc[0, 'L3BP'] = round(bbp_list[-3], 3)
        result_df.loc[0, 'L4BP'] = round(bbp_list[-4], 3)


        return result_df

    @check_running_time
    def make_future_price(self):
        query = '''
                SELECT STOCKCODE, DATE
                    ,FORMAT((PA1-CLOSE)/(CLOSE+0.01),3) AS PRA1
                    ,FORMAT((PA3-CLOSE)/(CLOSE+0.01),3) AS PRA3
                    ,FORMAT((PA5-CLOSE)/(CLOSE+0.01),3) AS PRA5
                    ,FORMAT((PA10-CLOSE)/(CLOSE+0.01),3) AS PRA10
                    ,FORMAT((PA20-CLOSE)/(CLOSE+0.01),3) AS PRA20
                    ,FORMAT((PA60-CLOSE)/(CLOSE+0.01),3) AS PRA60

                    ,FORMAT((PA1-OPEN_TOMORROW)/ (OPEN_TOMORROW+0.01),3) AS PRO1
                    ,FORMAT((PA3-OPEN_TOMORROW)/ (OPEN_TOMORROW+0.01),3) AS PRO3
                    ,FORMAT((PA5-OPEN_TOMORROW)/ (OPEN_TOMORROW+0.01),3) AS PRO5
                    ,FORMAT((PA10-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRO10
                    ,FORMAT((PA20-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRO20
                    ,FORMAT((PA60-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRO60

                    ,FORMAT((PH1-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRH1
                    ,FORMAT((PH3-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRH3
                    ,FORMAT((PH5-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRH5
                    ,FORMAT((PH10-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRH10
                    ,FORMAT((PH20-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRH20
                    ,FORMAT((PH60-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRH60

                    ,FORMAT((PL1-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRL1
                    ,FORMAT((PL3-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRL3
                    ,FORMAT((PL5-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRL5
                    ,FORMAT((PL10-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRL10
                    ,FORMAT((PL20-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRL20
                    ,FORMAT((PL60-OPEN_TOMORROW)/(OPEN_TOMORROW+0.01),3) AS PRL60


                FROM
                (

                    SELECT STOCKCODE, DATE, CLOSE, OPEN, HIGH, LOW
                        , ROW_NUMBER() OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC) AS RN
                        , SUM(CLOSE) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS PA1
                        , SUM(CLOSE) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 3 PRECEDING AND 3 PRECEDING) AS PA3
                        , SUM(CLOSE) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 5 PRECEDING AND 5 PRECEDING) AS PA5
                        , SUM(CLOSE) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 10 PRECEDING AND 10 PRECEDING) AS PA10
                        , SUM(CLOSE) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 20 PRECEDING AND 20 PRECEDING) AS PA20
                        , SUM(CLOSE) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 60 PRECEDING AND 60 PRECEDING) AS PA60

                        , SUM(OPEN) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS OPEN_TOMORROW


                        , MAX(HIGH) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS PH1
                        , MIN(LOW) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS PL1

                        , MAX(HIGH) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS PH3
                        , MIN(LOW) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS PL3

                        , MAX(HIGH) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS PH5
                        , MIN(LOW) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS PL5

                        , MAX(HIGH) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS PH10
                        , MIN(LOW) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS PL10

                        , MAX(HIGH) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS PH20
                        , MIN(LOW) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS PL20

                        , MAX(HIGH) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS PH60
                        , MIN(LOW) OVER (PARTITION BY STOCKCODE ORDER BY DATE DESC ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS PL60

                    FROM jazzdb.T_STOCK_OHLC_DAY A
                    JOIN jazzdb.T_DATE_INDEXED B USING (DATE)
                    JOIN jazzdb.T_STOCK_CODE_MGMT USING (STOCKCODE)
                    WHERE 1=1
                    AND LISTED = 1
                    AND B.CNT BETWEEN 0 AND 61
                    AND STOCKCODE = '%s'
                ) RS;
                ;


                    ''' % (self.stockcode)

        return db.selectpd(query)

    @check_running_time
    def make_moving_average(self):

        query = '''
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
        
        '''%(self.stockcode, self.today)

        return db.selectpd(query)


    @check_running_time
    def make_smar(self, n=[5,20,60,120,240]):

        df = db.selectpd("""
            SELECT STOCKCODE, CAST(DATE AS CHAR) AS DATE, A.OPEN, A.HIGH, A.LOW, A.CLOSE, B.VOLUME 
            FROM jazzdb.T_STOCK_OHLC_DAY A 
            JOIN jazzdb.T_STOCK_SND_DAY B USING(STOCKCODE, DATE) 
            WHERE 1=1
            AND STOCKCODE = '%s' 
            AND DATE > '%s'
            """ % (self.stockcode, self.from_date))

        for each in n:
            df['PSMA'+str(each)]=df['CLOSE'].rolling(each).mean()
            df['VSMA'+str(each)]=df['VOLUME'].rolling(each).mean()
            df['PSMAR' + str(each)] = (df['CLOSE'] - df['CLOSE'].rolling(each).mean()) / df['CLOSE'].rolling(each).mean()
            df['VSMAR' + str(each)] = (df['VOLUME'] - df['VOLUME'].rolling(each).mean()) / df['VOLUME'].rolling(each).mean()

        return df[['STOCKCODE', 'DATE', 'PSMAR5','PSMAR20', 'PSMAR60', 'PSMAR120', 'PSMAR240', 'VSMAR5','VSMAR20','VSMAR60', 'VSMAR120', 'VSMAR240']].round(3).tail(1)


    @check_running_time
    def make_snd_shortterm(self):
        qa = '''

            SELECT cast(DATE AS CHAR) AS DATE, STOCKCODE, CLOSE, VOLUME, FOREI, INS, PER, FINAN,
               SAMO, YG, TUSIN, INSUR, NATION, BANK, OTHERFINAN,
               OTHERCORPOR, OTHERFOR, CNT
            FROM jazzdb.T_STOCK_SND_DAY A
            JOIN jazzdb.T_DATE_INDEXED B USING (DATE)
            WHERE 1=1
            AND A.STOCKCODE = '%s'
            AND B.CNT BETWEEN 0 AND 500

            ''' % (self.stockcode)

        qb = '''

            SELECT STOCKCODE, SHARE
            FROM jazzdb.T_STOCK_SHARES_INFO
            WHERE 1=1
            AND HOLDER = '유통주식수'
            AND STOCKCODE = '%s'
            AND DATE = '%s'

            ''' % (self.stockcode, self.to_date)

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


        return adf[['STOCKCODE', 'DATE', 'CLOSE', 'P1', 'P3', 'P5', 'P20', 'P60', 'I1',
                         'I3', 'I5', 'I20', 'I60', 'F1', 'F3', 'F5', 'F20', 'F60', 'PS1', 'PS3',
                         'PS5', 'PS20', 'PS60', 'FN1', 'FN3', 'FN5', 'FN20', 'FN60', 'YG1',
                         'YG3', 'YG5', 'YG20', 'YG60', 'S1', 'S3', 'S5', 'S20', 'S60', 'T1',
                         'T3', 'T5', 'T20', 'T60', 'IS1', 'IS3', 'IS5', 'IS20', 'IS60', 'NT1',
                         'NT3', 'NT5', 'NT20', 'NT60', 'BK1', 'BK3', 'BK5', 'BK20', 'BK60',
                         'OC1', 'OC3', 'OC5', 'OC20', 'OC60', 'IR', 'FR', 'PR', 'FNR', 'YR',
                         'SR', 'TR', 'ISR', 'NTR', 'BKR', 'OCR']].tail(1)


    @check_running_time
    def make_snd_longterm_to_be(self):
        qa = '''

        SELECT cast(DATE AS CHAR) AS DATE, STOCKCODE, CLOSE, VOLUME, FOREI, INS, PER, FINAN,
           SAMO, YG, TUSIN, INSUR, NATION, BANK, OTHERFINAN,
           OTHERCORPOR, OTHERFOR, CNT
        FROM jazzdb.T_STOCK_SND_DAY A
        JOIN jazzdb.T_DATE_INDEXED B USING (DATE)
        WHERE 1=1
        AND A.STOCKCODE = '%s'
        AND B.CNT BETWEEN 0 AND 500

        ''' % (self.stockcode)

        qb = '''

        SELECT STOCKCODE, SHARE
        FROM jazzdb.T_STOCK_SHARES_INFO
        WHERE 1=1
        AND HOLDER = '유통주식수'
        AND STOCKCODE = '%s'
        AND DATE = '%s'

        ''' % (self.stockcode, self.to_date)



        dic = {

            'INS': 'I',
            'FOREI': 'F',
            'YG': 'YG',
            'SAMO': 'S',
            'PER': 'PS',

        }

        adf = db.selectpd(qa)
        bdf = db.selectpd(qb)

        winsize = [80, 120, 160, 200, 240]

        adf.CLOSE = abs(adf.CLOSE)

        # 수급퍼센티지 만들기
        for eachwin in dic.keys():
            for eachsize in winsize:
                adf[dic[eachwin] + str(eachsize)] = adf[eachwin].rolling(eachsize).sum() / bdf.SHARE.values[0]


        # 주가변동퍼센티지
        for eachsize in winsize:
            adf['P' + str(eachsize)] = adf.CLOSE.pct_change(periods=eachsize)

        return adf[['STOCKCODE', 'DATE',
                    'I80', 'I120', 'I160', 'I200', 'I240',
                    'F80', 'F120', 'F160', 'F200', 'F240',
                    'P80', 'P120', 'P160', 'P200', 'P240',
                    'YG80', 'YG120', 'YG160', 'YG200', 'YG240',
                    'S80', 'S120', 'S160', 'S200', 'S240',
                    'PS80', 'PS120', 'PS160', 'PS200', 'PS240']].tail(1)

    @check_running_time
    def make_snd_longterm_as_is(self):

        query = '''
        SELECT RS.STOCKCODE, RS.DATE 

            ,ROUND(I80/SHARE,5) AS I80
            ,ROUND(I120/SHARE,5) AS I120
            ,ROUND(I160/SHARE,5) AS I160
            ,ROUND(I200/SHARE,5) AS I200
            ,ROUND(I240/SHARE,5) AS I240
        
            ,ROUND(F80/SHARE,5) AS F80
            ,ROUND(F120/SHARE,5) AS F120
            ,ROUND(F160/SHARE,5) AS F160
            ,ROUND(F200/SHARE,5) AS F200
            ,ROUND(F240/SHARE,5) AS F240
        
            ,ROUND((CLOSE-C80)/C80,5) AS P80
            ,ROUND((CLOSE-C120)/C120,5) AS P120
            ,ROUND((CLOSE-C160)/C160,5) AS P160
            ,ROUND((CLOSE-C200)/C200,5) AS P200
            ,ROUND((CLOSE-C240)/C240,5) AS P240
            
            ,ROUND(YG80/SHARE,5) AS YG80
            ,ROUND(YG120/SHARE,5) AS YG120
            ,ROUND(YG160/SHARE,5) AS YG160
            ,ROUND(YG200/SHARE,5) AS YG200
            ,ROUND(YG240/SHARE,5) AS YG240
        
            ,ROUND(S80/SHARE,5) AS S80
            ,ROUND(S120/SHARE,5) AS S120
            ,ROUND(S160/SHARE,5) AS S160
            ,ROUND(S200/SHARE,5) AS S200
            ,ROUND(S240/SHARE,5) AS S240
        
            ,ROUND(PS80/SHARE,5) AS PS80
            ,ROUND(PS120/SHARE,5) AS PS120
            ,ROUND(PS160/SHARE,5) AS PS160
            ,ROUND(PS200/SHARE,5) AS PS200
            ,ROUND(PS240/SHARE,5) AS PS240
        
        FROM
        
        (
        
            SELECT A.STOCKCODE, A.DATE, ABS(A.CLOSE) AS CLOSE, C.SHARE+0.01 AS SHARE, B.CNT
        
                , SUM(ABS(A.CLOSE)) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 80 PRECEDING AND 80 PRECEDING) AS C80
                , SUM(ABS(A.CLOSE)) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 120 PRECEDING AND 120 PRECEDING) AS C120
                , SUM(ABS(A.CLOSE)) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 160 PRECEDING AND 160 PRECEDING) AS C160
                , SUM(ABS(A.CLOSE)) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 200 PRECEDING AND 200 PRECEDING) AS C200
                , SUM(ABS(A.CLOSE)) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 240 PRECEDING AND 240 PRECEDING) AS C240
        
        
                # 3,5,10,20 누적 매수
                , A.INS AS I1	
                , SUM(A.INS) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 79 PRECEDING AND CURRENT ROW) AS I80
                , SUM(A.INS) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS I120
                , SUM(A.INS) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 159 PRECEDING AND CURRENT ROW) AS I160
                , SUM(A.INS) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS I200
                , SUM(A.INS) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS I240
        
                , A.FOREI AS F1
                , SUM(A.FOREI) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 79 PRECEDING AND CURRENT ROW) AS F80
                , SUM(A.FOREI) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS F120
                , SUM(A.FOREI) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 159 PRECEDING AND CURRENT ROW) AS F160
                , SUM(A.FOREI) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS F200
                , SUM(A.FOREI) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS F240
                
                
                , A.YG AS YG1
                , SUM(A.YG) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 79 PRECEDING AND CURRENT ROW) AS YG80
                , SUM(A.YG) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS YG120
                , SUM(A.YG) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 159 PRECEDING AND CURRENT ROW) AS YG160
                , SUM(A.YG) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS YG200
                , SUM(A.YG) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS YG240
        
                , A.SAMO AS S1
                , SUM(A.SAMO) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 79 PRECEDING AND CURRENT ROW) AS S80
                , SUM(A.SAMO) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS S120
                , SUM(A.SAMO) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 159 PRECEDING AND CURRENT ROW) AS S160
                , SUM(A.SAMO) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS S200
                , SUM(A.SAMO) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS S240
        
                , A.PER AS PS1
                , SUM(A.PER) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 79 PRECEDING AND CURRENT ROW) AS PS80
                , SUM(A.PER) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS PS120
                , SUM(A.PER) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 159 PRECEDING AND CURRENT ROW) AS PS160
                , SUM(A.PER) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS PS200
                , SUM(A.PER) OVER (PARTITION BY A.STOCKCODE ORDER BY DATE ASC ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) AS PS240
        
            FROM jazzdb.T_STOCK_SND_DAY A
            JOIN jazzdb.T_DATE_INDEXED B USING (DATE)
        
            LEFT JOIN (
        
                    SELECT STOCKCODE, SHARE, DATE
                    FROM
                    (
                        SELECT STOCKCODE, SHARE, DATE
                        FROM jazzdb.T_STOCK_SHARES_INFO
                        WHERE 1=1
                        AND HOLDER = '유통주식수'
                    ) T1
        
                    WHERE 1=1
        
            ) C USING (STOCKCODE, DATE)
        
            WHERE 1=1
            AND A.STOCKCODE = '%s'
            AND B.CNT < 800
        ) RS
        
        WHERE SHARE IS NOT NULL
        AND RS.CNT=0
        
        
        ;

        
        
        '''%(self.stockcode)

        df = db.selectpd(query)

        return df

    @check_running_time
    def make_vwap(self):

        df_day = db.selectpd('''
    
        SELECT A.STOCKCODE, cast(A.DATE AS CHAR) as DATE, A.OPEN, A.HIGH, A.LOW, A.CLOSE, B.VOLUME, A.VALUE AS TRADING_VALUE, MA5
        FROM jazzdb.T_STOCK_OHLC_DAY A
        JOIN jazzdb.T_STOCK_SND_DAY B ON (A.STOCKCODE = B.STOCKCODE AND A.DATE = B.DATE)
        JOIN jazzdb.T_DATE_INDEXED C ON (A.DATE = C.DATE)
        JOIN jazzdb.T_STOCK_MA D ON (A.DATE = D.DATE AND D.STOCKCODE= A.STOCKCODE)
        WHERE 1=1
        AND A.STOCKCODE = '%s'
        AND A.DATE BETWEEN '%s' AND '%s'
        
    
        ''' % (self.stockcode, self.from_date, self.to_date))

        df_day["TYPICAL_PRICE"] = (df_day["HIGH"] + df_day["LOW"] + df_day["CLOSE"]) / 3

        df_day["TP_MUL_VOLUME"] = df_day["TYPICAL_PRICE"] * df_day["VOLUME"]

        df_day["VOLUME_SUM_3"] = df_day["VOLUME"].rolling(3).sum()
        df_day["VOLUME_SUM_5"] = df_day["VOLUME"].rolling(5).sum()
        df_day["VOLUME_SUM_10"] = df_day["VOLUME"].rolling(10).sum()
        df_day["VOLUME_SUM_20"] = df_day["VOLUME"].rolling(20).sum()
        df_day["VOLUME_SUM_60"] = df_day["VOLUME"].rolling(60).sum()
        df_day["VOLUME_SUM_120"] = df_day["VOLUME"].rolling(120).sum()

        df_day["TP_MUL_VOLUME_SUM_3"] = df_day["TP_MUL_VOLUME"].rolling(3).sum()
        df_day["TP_MUL_VOLUME_SUM_5"] = df_day["TP_MUL_VOLUME"].rolling(5).sum()
        df_day["TP_MUL_VOLUME_SUM_10"] = df_day["TP_MUL_VOLUME"].rolling(10).sum()
        df_day["TP_MUL_VOLUME_SUM_20"] = df_day["TP_MUL_VOLUME"].rolling(20).sum()
        df_day["TP_MUL_VOLUME_SUM_60"] = df_day["TP_MUL_VOLUME"].rolling(60).sum()
        df_day["TP_MUL_VOLUME_SUM_120"] = df_day["TP_MUL_VOLUME"].rolling(120).sum()

        df_day["VWAP3"] = (df_day["TP_MUL_VOLUME_SUM_3"] / df_day["VOLUME_SUM_3"]).round(0)
        df_day["VWAP5"] = (df_day["TP_MUL_VOLUME_SUM_5"] / df_day["VOLUME_SUM_5"]).round(0)
        df_day["VWAP10"] = (df_day["TP_MUL_VOLUME_SUM_10"] / df_day["VOLUME_SUM_10"]).round(0)
        df_day["VWAP20"] = (df_day["TP_MUL_VOLUME_SUM_20"] / df_day["VOLUME_SUM_20"]).round(0)
        df_day["VWAP60"] = (df_day["TP_MUL_VOLUME_SUM_60"] / df_day["VOLUME_SUM_60"]).round(0)
        df_day["VWAP120"] = (df_day["TP_MUL_VOLUME_SUM_120"] / df_day["VOLUME_SUM_120"]).round(0)

        df_day["DIFF_VWAP3_CLOSE"] = (df_day["VWAP3"] - df_day["CLOSE"])/df_day["CLOSE"]
        df_day["DIFF_VWAP5_CLOSE"] = (df_day["VWAP5"] - df_day["CLOSE"])/df_day["CLOSE"]
        df_day["DIFF_VWAP10_CLOSE"] = (df_day["VWAP10"] - df_day["CLOSE"])/df_day["CLOSE"]
        df_day["DIFF_VWAP20_CLOSE"] = (df_day["VWAP20"] - df_day["CLOSE"])/df_day["CLOSE"]
        df_day["DIFF_VWAP60_CLOSE"] = (df_day["VWAP60"] - df_day["CLOSE"])/df_day["CLOSE"]
        df_day["DIFF_VWAP120_CLOSE"] = (df_day["VWAP120"] - df_day["CLOSE"])/df_day["CLOSE"]

        # print(df_day[['STOCKCODE', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'TYPICAL_PRICE', 'TP_MUL_VOLUME', 'VOLUME_SUM_5', 'VWAP5', 'MA5', 'DIFF_VWAP5_CLOSE']].tail(1))
        return df_day[['STOCKCODE', 'DATE', 'VWAP3', 'VWAP5', 'VWAP10', 'VWAP20', 'VWAP60', 'VWAP120']]


    # info(datetime.now()-start)




if __name__ == '__main__':

    get_all_stocks()
    today, from_date, date_list = get_date_from_to()

    # TEST
    CrawlStockObject(stockcode='093320', from_date = from_date, to_date=today).main()

    # PROD



    # for i, eachcode in enumerate(codeDic.keys()):
    #     try:
    #         makebb(eachcode, today)
    #         if i % 500 == 0:
    #             print(i, eachcode, 'DONE')
    #     except Exception as e:
    #         print(i, eachcode, e)


