import jazzstock_bot.common.connector_db as db
import pandas as pd
from datetime import datetime

pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False

class stock_bb_event:
    '''
    조건식 만드는 객체
    '''
    def __init__(self, stockcode,
                 the_date,
                 start_date):

        self.stockcode = stockcode
        self.the_date = the_date
        self.start_date = start_date


    def get_bb(self):

        self.df = db.selectpd(f'''
        SELECT DATE, BBU, BBL, BBP, BBW
        FROM jazzdb.T_STOCK_BB
        WHERE 1=1
        AND STOCKCODE = "{self.stockcode}" 
        AND DATE BETWEEN "{self.start_date}" AND "{self.the_date}"
        ''')

    def get_bb_event(self, threshold=0, mid = 0.5):


        bbp = self.df.BBP.values
        bbw = self.df.BBW.values
        date = self.df.DATE.values

        prev_point = 0
        prev_event = '-'
        curr_event = None

        ret_df = pd.DataFrame(columns=['PREV_IDX', 'DATE_IDX',
                                       'DATE_FROM', 'DATE_TO', 'INTERVAL',
                                       'PREV_EVENT', 'CURR_EVENT','BBP','BBW'])


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
                ret_df.loc[len(ret_df)] = [prev_point, i+1,
                                           date[prev_point], date[i + 1], i + 1 - prev_point,
                                           prev_event, curr_event,
                                           bbp[i + 1], bbw[i + 1]]
                prev_event = curr_event
                prev_point = i+1

            elif i+1 == len(bbp)-1:
                ret_df.loc[len(ret_df)] = [prev_point, i+1,
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
        result_df.loc[0, 'DATE'] = self.the_date

        result_df.loc[0, 'DIR_L1'] = '%s'%(prev_event_list[-1])
        result_df.loc[0, 'DIR_L2'] = '%s' % (prev_event_list[-2])
        result_df.loc[0, 'DIR_L3'] = '%s'%(prev_event_list[-3])
        result_df.loc[0, 'DIR_L4'] = '%s'%(prev_event_list[-4])

        result_df.loc[0, 'DAYS_L1'] = int(59 - interval_list[-1])
        result_df.loc[0, 'DAYS_L2'] = int(59 - interval_list[-2])
        result_df.loc[0, 'DAYS_L3'] = int(59 - interval_list[-3])
        result_df.loc[0, 'DAYS_L4'] = int(59 - interval_list[-4])

        result_df.loc[0, 'BBW_L1'] = round(bbw_list[-1],3)
        result_df.loc[0, 'BBW_L2'] = round(bbw_list[-2],3)
        result_df.loc[0, 'BBW_L3'] = round(bbw_list[-3],3)
        result_df.loc[0, 'BBW_L4'] = round(bbw_list[-4],3)

        result_df.loc[0, 'BBP_L1'] = round(bbp_list[-1],3)
        result_df.loc[0, 'BBP_L2'] = round(bbp_list[-2],3)
        result_df.loc[0, 'BBP_L3'] = round(bbp_list[-3],3)
        result_df.loc[0, 'BBP_L4'] = round(bbp_list[-4],3)
        
        # print(result_df)
        db.insertdf(result_df, table='jazzdb.T_STOCK_BB_EVENT')

def get_date_by_cnt(cnt):
    date = db.selectSingleValue(
        '''SELECT CAST(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = "%s"''' % (cnt))
    return date



def db_readAll(dt):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_OHLC_DAY
                            WHERE DATE = '%s'
                            GROUP BY STOCKCODE
                        )
                        AND A.LISTED = 1
                                                        """ % (dt)

    return db.selectSingleColumn(query)

if __name__=='__main__':


    for the_date_cnt in range(0, 1):
        the_date = get_date_by_cnt(the_date_cnt)
        start_date = get_date_by_cnt(the_date_cnt +  59)
        stockcodes = db_readAll(the_date)
        for j, stockcode in enumerate(stockcodes):
            try:
                print(j, stockcode, len(stockcodes))
                s = datetime.now()
                stock = stock_bb_event(stockcode=stockcode, the_date=the_date, start_date=start_date)
                stock.get_bb()
                stock.get_bb_event()
                if j % 100 == 0:
                    print(the_date, j, stockcode, datetime.now()-s)
            except Exception as e:
                # print(e)
                pass