import jazzstock_bot.common.connector_db as db
import pandas as pd
import numpy as np
import requests
from datetime import datetime

pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False


'''

QUARTER 와 DATE를 조인할 수 있는 컬럼을 생성하는 스크립트
매 거래일데이터가 추가될때 마다 실행되어야함

'''

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

class GenFinanFeature:

    def __init__(self, today_date):
        self.today_date = today_date
        self.quarter_period = {  '12':('03-01', '05-15'),  # 이기간 동안은 작년 4Q를
                                 '03':('05-15', '08-14'),  # 이기간 동안은 올해 1Q를
                                 '06':('08-14', '11-15'),  # 이기간 동안은 올해 2Q를
                                 '09': ('11-15', '03-01')}  # 이기간 동안은 작년 3Q를

        self.quarter_previous = None
        self.quarter_current = None

        self.stockcodes_all = []
        self.stockcodes_not_updated = []
        self.stockcodes_to_update = []

    def usage(self):
        usage = '''
        
        매 거래일마다 수집후 자동실행되는 스크립트
        불규칙적으로 update되는 itooza사이트의 재무데이터 특성상 수집이 반복적으로 실행되어야함
        
        '''

    def run(self):
        self.gen_quarter()
        quarter_dict = self.get_quarter(self.today_date)

        self.quarter_current = quarter_dict.get("quarter_current")
        self.quarter_previous = quarter_dict.get("quarter_previous")

        self.stockcodes_all = self.get_stockcodes_all()
        # =========================================================================
        self.stockcodes_crawled_already = self.get_stockcodes_crawled_already()
        self.stockcodes_to_update = self.get_stockcodes_to_update()

        for i, stockcode in enumerate(self.stockcodes_to_update):
            ret_itooza = self.crawl_itooza(stockcode)
            if ret_itooza: ## 수집된 결과가 있음, na값으로 업데이트 친경우 이쪽로직은 계속 돌아갈것...
                ret_xps_gen = self.get_xps_xox(stockcode)
                print("%s - %s - %s/%s - %s"%(self.__class__.__name__, 1, i, len(self.stockcodes_to_update), stockcode), ret_xps_gen)
            else: ## 수집된 결과가 없음
                print("%s - %s - %s/%s - %s"%(self.__class__.__name__, 0, i, len(self.stockcodes_to_update), stockcode))
        # =========================================================================

        # self.stockcodes_xox_crawled_already = self.get_stockcodes_xox_crawled_already()
        # self.stockcodes_xox_to_update = self.get_stockcodes_yoy_to_update()
        #
        # for i, stockcode in enumerate(self.stockcodes_xox_to_update):
        #     ret_xps_gen = self.get_xps_xox(stockcode)
        #     if ret_xps_gen:
        #         print("%s - %s - %s/%s - %s" % (self.__class__.__name__, 1, i, len(self.stockcodes_xox_to_update), stockcode))
        #     else:
        #         print("%s - %s - %s/%s - %s" % (self.__class__.__name__, 0, i, len(self.stockcodes_xox_to_update), stockcode))




    def gen_quarter(self):
        '''
        UPDATE jazzdb.T_DATE_FINAN
        '''
        query = '''
        
        SELECT CAST(DATE AS CHAR) AS DATE, CAST(SUBSTRING(DATE, 3,2) AS UNSIGNED) AS YY , SUBSTRING(DATE, 6,5) AS MM_DD
        FROM
        (
            SELECT DATE, CNT, QUARTER
            FROM jazzdb.T_DATE_INDEXED
            LEFT JOIN jazzdb.T_DATE_FINAN USING (DATE)
            ORDER BY DATE DESC
        ) A
        WHERE 1=1
        AND A.QUARTER IS NULL
        AND A.CNT < 500
        
        
        '''

        df = db.selectpd(query)

        if len(df)>0:

            for q in self.quarter_period.keys():
                if q in ['03', '06']:
                    df.loc[(df['MM_DD'] > self.quarter_period[q][0]) & (df['MM_DD'] <= self.quarter_period[q][1]), 'PY'] = (df['YY'])
                    df.loc[(df['MM_DD'] > self.quarter_period[q][0]) & (df['MM_DD'] <= self.quarter_period[q][1]), 'QQ'] = q
                elif q == '12':
                    df.loc[(df['MM_DD'] > self.quarter_period[q][0]) & (df['MM_DD'] <= self.quarter_period[q][1]), 'PY'] = (df['YY'] - 1)
                    df.loc[(df['MM_DD'] > self.quarter_period[q][0]) & (df['MM_DD'] <= self.quarter_period[q][1]), 'QQ'] = q
                else:
                    df.loc[(df['MM_DD'] > self.quarter_period[q][0]) | (df['MM_DD'] <= self.quarter_period[q][1]), 'PY'] = (df['YY'] - 1)
                    df.loc[(df['MM_DD'] > self.quarter_period[q][0]) | (df['MM_DD'] <= self.quarter_period[q][1]), 'QQ'] = q

            df.PY = df.PY.astype(int)
            df["QUARTER"] = df["PY"].astype(str) + df["QQ"]
            is_df = df[['DATE', 'QUARTER']].copy()

            if len(is_df) > 0 :
                db.insertdf(df[['DATE', 'QUARTER']],'jazzdb.T_DATE_FINAN')


    def get_quarter(self, date):

        yy = int(str(date)[:4][2:])
        mm_dd = str(date)[5:]


        quarter_current = None
        quarter_year = None
        quarter_quarter = None
        for q, v in self.quarter_period.items():
            if mm_dd > self.quarter_period[q][0] and mm_dd <= self.quarter_period[q][1]:
                if q in ['03', '06']:

                    quarter_year = yy
                    quarter_quarter = q
                    break
                else:
                    quarter_year = yy-1
                    quarter_quarter = q

                    break

        if quarter_year is None and quarter_quarter is None:
            quarter_year = yy-1
            quarter_quarter = q

        quarter_current = '%s%s'%(quarter_year, quarter_quarter)

        if quarter_quarter in ['06', '09', '12']:
            quarter_previous = '%s%02d'%(quarter_year, int(quarter_quarter)-3)
        else:
            quarter_previous = '%s12' % (quarter_year-1)
        return {"quarter_current":quarter_current, "quarter_previous": quarter_previous}

    def get_stockcodes_all(self):
        stockcodes = db.selectSingleColumn('''
        SELECT DISTINCT STOCKCODE
        FROM jazzdb.T_STOCK_FINAN
        WHERE QUARTER = '2103'
        AND TYPE='C'
        ''')
        return stockcodes

    def get_stockcodes_crawled_already(self):
        stockcodes_crawled_already = db.selectSingleColumn('''
        SELECT DISTINCT STOCKCODE
        FROM jazzdb.T_STOCK_FINAN
        WHERE QUARTER = '2106'
        AND TYPE = 'C'
        AND (ROE != -1 AND OPR != -1)
        ''')
        return stockcodes_crawled_already

    def get_stockcodes_xox_crawled_already(self):
        stockcodes_xox_crawled_already = db.selectSingleColumn('''
        SELECT DISTINCT STOCKCODE
        FROM jazzdb.T_STOCK_FINAN_XOX
        WHERE QUARTER = '2106'
        ''')
        return stockcodes_xox_crawled_already

    def get_stockcodes_to_update(self):
        stockcodes_to_update = []
        for stockcode in self.stockcodes_all:
            if stockcode not in self.stockcodes_crawled_already:
                stockcodes_to_update.append(stockcode)
        return stockcodes_to_update

    def get_stockcodes_yoy_to_update(self):
        stockcodes_to_update = []
        for stockcode in self.stockcodes_all:
            if stockcode not in self.stockcodes_xox_crawled_already:
                stockcodes_to_update.append(stockcode)
        return stockcodes_to_update


    def crawl_itooza(self, stockcode):
        url = "https://search.itooza.com/search.htm?seName=%s&cpv=#indexTable3" % (stockcode)
        response = requests.get(url, headers=headers)
        response.encoding = 'EUC-KR'
        html = response.text

        try:
            df_list = pd.read_html(html)

            ## 연환산

            df = pd.DataFrame(
                columns=['STOCKCODE', 'TYPE', 'QUARTER', 'EPSC', 'EPSI', 'PER', 'BPS', 'PBR', 'DV', 'DVR', 'ROE', 'NPR',
                         'OPR'])

            _c = df_list[2].transpose()
            _c.reset_index(inplace=True)
            for row in _c.values[1:]:
                df.loc[len(df)] = [stockcode, 'c'] + row[:-1].tolist()

            ## 분기

            _q = df_list[4].transpose()
            _q.reset_index(inplace=True)
            for row in _q.values[1:]:
                df.loc[len(df)] = [stockcode, 'q'] + row[:-1].tolist()

            ## 연간

            _y = df_list[3].transpose()
            _y.reset_index(inplace=True)
            for row in _y.values[1:]:
                df.loc[len(df)] = [stockcode, 'y'] + row[:-1].tolist()

            df = df.fillna(-1)

            df.QUARTER = df.QUARTER.str.replace('월', '', regex=False)
            df.QUARTER = df.QUARTER.str.replace('.', '', regex=False)

            df = df[
                ['STOCKCODE', 'QUARTER', 'TYPE', 'EPSC', 'EPSI', 'PER', 'BPS', 'PBR', 'DV', 'DVR', 'ROE', 'NPR', 'OPR']]


            quarter_current = self.quarter_current
            df_current = df[df['QUARTER'].isin([quarter_current])]

            if len(df_current) > 0:



                '''
                
                업데이트 되지 않은 예시:
                
                   STOCKCODE  DATE TYPE  EPSC  EPSI  PER  BPS  PBR   DV  DVR  ROE  NPR   OPR
                0     000020  2106    c   0.0  -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0  9.38
                12    000020  2106    q  -1.0  -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0  9.50
                24    000020  2106    y  -1.0  -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0  9.38
                
                
                '''

                if not (df_current.values[0][4:7].tolist() == [-1, -1, -1]):
                    print(df_current)
                    query_delete = 'DELETE FROM jazzdb.T_STOCK_FINAN WHERE STOCKCODE="%s" AND QUARTER = "%s"' % (
                    stockcode, quarter_current)
                    db.delete(query_delete)
                    db.insertdf(df[df['QUARTER'].isin([quarter_current])], 'jazzdb.T_STOCK_FINAN')
                    return True

            return False
            ## IF CHANGED OR NOT EXISTS DO UPDATE 를 구현하도록 !

        except Exception as e:
            print('@', e)



    def get_xps_xox(self, stockcode):
        '''

        FINAN 테이블을 참조하여
        분기 재무 데이터 YOY, QOQ 데이터를 생성하는 함

        '''


        query = '''
    
        SELECT STOCKCODE, QUARTER, EPSC, EPSI, BPS
        FROM jazzdb.T_STOCK_FINAN
        WHERE 1=1
        AND STOCKCODE = '%s'
        AND EPSC != -1
        AND ROE != -1
        AND TYPE = 'c'
    
        '''%(stockcode)

        df_yoy = db.selectpd(query)

        query = '''
    
        SELECT STOCKCODE, QUARTER, EPSC, EPSI, BPS
        FROM jazzdb.T_STOCK_FINAN
        WHERE 1=1
        AND STOCKCODE = '%s'
        AND EPSC != -1
        AND ROE != -1
        AND TYPE = 'q'
    
        '''%(stockcode)

        df_qoq = db.selectpd(query)



        if len(df_yoy)>0:

            features = ['EPSC', 'EPSI', 'BPS']
            column_list = []
            for feature in features:
                df_yoy['%s_YOY' % (feature)] = ((df_yoy[feature] - df_yoy[feature].shift(4)) / df_yoy[feature].shift(4)).round(3)
                df_qoq['%s_QOQ' % (feature)] = ((df_qoq[feature] - df_qoq[feature].shift(1)) / df_qoq[feature].shift(1)).round(3)
                column_list.append('%s_YOY' % (feature))
                column_list.append('%s_QOQ' % (feature))

            df_xps = df_yoy.merge(df_qoq, on=['STOCKCODE', 'QUARTER'])
            df_xps = df_xps.replace([np.inf, -np.inf], -999)

            if df_xps is not None and isinstance(df_xps, pd.DataFrame):

                db.delete("DELETE FROM jazzdb.T_STOCK_FINAN_XOX WHERE STOCKCODE = '%s' AND QUARTER = '%s'"%(stockcode, self.quarter_current))
                df_xps = df_xps.dropna()
                df_xps_current_quarter = df_xps[df_xps['QUARTER'] == self.quarter_current]
                df_xps_current_quarter = df_xps_current_quarter[['STOCKCODE', 'QUARTER', 'EPSC_YOY', 'EPSC_QOQ', 'EPSI_YOY', 'EPSI_QOQ', 'BPS_YOY', 'BPS_QOQ']]
                if len(df_xps_current_quarter) > 0 and df_xps_current_quarter.EPSC_YOY.values[0] != -1:
                    db.insertdf(df_xps_current_quarter, 'jazzdb.T_STOCK_FINAN_XOX')
                    return True

        return False
if __name__ == "__main__":

    today = datetime.now().date()
    obj = GenFinanFeature(today)
    obj.run()