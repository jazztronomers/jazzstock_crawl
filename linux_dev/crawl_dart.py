import dart_fss as dart
import pandas as pd
import numpy as np
import json
import warnings
from datetime import datetime
import jazzstock_bot.common.connector_db as db


warnings.filterwarnings("ignore", category=DeprecationWarning)
np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)


pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False

api_key="e55c8c15a652164b57fd8c01193ff4a26e21f769" # api key 변수 설정

class DartParser:


    def __init__(self, stockcode, from_date_searching, to_date_searching, year, target_quarter):
        global corp_list


        self.stockcode = stockcode
        self.report_type = quarter_dic.get(target_quarter).get('type')
        self.month = quarter_dic.get(target_quarter).get('month')

        self.from_date = quarter_dic.get(target_quarter).get('from_date').replace('-','')
        self.to_date = quarter_dic.get(target_quarter).get('to_date').replace('-','')

        self.year = year
        self.from_date_searching = from_date_searching
        self.to_date_searching = to_date_searching
        self.target_quarter = target_quarter

        self.dart_stock = corp_list.find_by_stock_code(stockcode)
        self.dart_stock_dict = self.dart_stock.to_dict()
        self.corp_code = self.dart_stock_dict.get("corp_code")
        self.sector = self.dart_stock_dict.get('sector')
        self.product = self.dart_stock_dict.get('product')


    def get_result(self):
        try:
            xbrl = self.get_xbrl()
            result = self.parse_xbrl(xbrl)
            return result
        except Exception as e:
            print(e)
            return None


    def get_xbrl(self):

        reports = dart.filings.search(corp_code=self.corp_code, bgn_de=self.from_date_searching, end_de=self.to_date_searching,
                                      pblntf_detail_ty=['a001', 'a002', 'a003'])

        # for i, each_report in enumerate(reports.to_dict().get('report_list')):
        #     print(i, each_report['report_nm'])

        report = reports[0]
        xbrl = report.xbrl
        return xbrl

    def parse_xbrl(self, xbrl):

        self.exist_consolidated = xbrl.exist_consolidated()

        # 연결 현금흐름표 추출 (리스트 반환)
        finantial_statement = xbrl.get_financial_statement()
        finantial_statement = finantial_statement[0]
        df_fs = finantial_statement.to_DataFrame(show_class=False, show_abstract=False)

        income_statement = xbrl.get_income_statement()
        income_statement = income_statement[0]
        df_is = income_statement.to_DataFrame(show_class=False, show_abstract=False)




        # 연결 현금프름표
        cash_flows = xbrl.get_cash_flows()
        cash_flows = cash_flows[0]
        df_cf = cash_flows.to_DataFrame(show_class=False, show_abstract=False)

        # for each_frame in df_fs, df_is, df_cf:
        #     for j, column in enumerate(each_frame.columns.tolist()):
        #         print(j, column)
        #     print('-'*100)

        idx_connectd_fs, idx_independent_fs = 3, 4
        for i, each_lv in enumerate(df_fs.columns[3:].tolist()):
            end_date = each_lv[0]
            title = each_lv[1][0]

            if '%s%s'%(self.year, self.month) in end_date and "연결" in title:
                idx_connectd_fs = 3 + i
                break



        idx_connectd_is, idx_independent_is = 3, 4
        for i, each_lv in enumerate(df_is.columns[3:].tolist()):

            from_date = each_lv[0].split('-')[0]
            to_date = each_lv[0].split('-')[1]
            title = each_lv[1][0]

            if self.target_quarter=='4Q' and self.to_date in to_date and "연결" in title:
                idx_connectd_is = 3 + i
                break
            elif self.from_date in from_date and \
               self.to_date in to_date and \
                    "연결" in title:
                idx_connectd_is = 3 + i
                break



        df_is = df_is[[df_is.columns[1], df_is.columns[2], df_is.columns[0], df_is.columns[idx_connectd_is]]]
        df_is.columns=['name', 'description', 'code', 'value']
        df_fs = df_fs[[df_fs.columns[1], df_fs.columns[2], df_fs.columns[0], df_fs.columns[idx_connectd_fs]]]
        df_fs.columns = ['name', 'description', 'code', 'value']

        features_dict = {
            'revenue': ['ifrs-full_Revenue', 'ifrs_Revenue'],
            'operating_income': ['dart_OperatingIncomeLoss'],
            'profit': ['ifrs-full_ProfitLoss', 'ifrs_ProfitLoss'],
            'profit_owner': ['ifrs-full_ProfitLossAttributableToOwnersOfParent',
                             'ifrs_ProfitLossAttributableToOwnersOfParent'],
            'book_value': ['ifrs-full_Equity', 'ifrs_Equity'],
            'book_value_owner': ['ifrs-full_EquityAttributableToOwnersOfParent',
                                 'ifrs_EquityAttributableToOwnersOfParent'],
        }

        try:




            code_list = df_is.code.values.tolist()
            code_list = code_list + df_fs.code.values.tolist()

            warn = False
            for k,vl in features_dict.items():

                temp = list(set(vl) & set(code_list))
                if len(temp)==0:
                    print('WARN - [ %s ] not in code_list'%(k))
                    warn=True

            if warn:
                print(df_is[['name', 'code']])
                print(df_fs[['name', 'code']])


            #
            # for k,vl in features_dict.items():
            #
            #     for v in vl:
            #         print(self.stockcode, self.year, self.month, v, df_is[df_is.code==v].value.values)


            # print(df_is)

            ## admin_expense    = df_is[df_is.name.isin(['판매비와관리비', '기타판매비와관리비'])].value.values[0]

            revenue = df_is[df_is.code.isin(['ifrs-full_Revenue', 'ifrs_Revenue'])].value.values
            operating_income = df_is[df_is.code=='dart_OperatingIncomeLoss'].value.values
            profit = df_is[df_is.code.isin(['ifrs-full_ProfitLoss','ifrs_ProfitLoss'])].value.values
            profit_owner     = df_is[df_is.code.isin(['ifrs-full_ProfitLossAttributableToOwnersOfParent',
                                                      'ifrs_ProfitLossAttributableToOwnersOfParent',
                                                      'entity00247975_udf_IS_2020812213457946_ProfitLoss',
                                                      'entity00247975_udf_IS_2020112201515138_ProfitLoss'])].value.values

            book_value = df_fs[df_fs.code.isin(['ifrs-full_Equity', 'ifrs_Equity'])].value.values
            book_value_owner = df_fs[df_fs.code.isin(['ifrs-full_EquityAttributableToOwnersOfParent', 'ifrs_EquityAttributableToOwnersOfParent'])].value.values

            ret = {
                "STOCKCODE":self.stockcode,
                "QUARTER":'%s%s'%(self.year[2:], self.month),
                "REVENUE":revenue[0] if bool(revenue) else np.nan,
                "OPERATE_INCOME":operating_income[0] if bool(operating_income) else np.nan,
                "PROFIT":profit[0] if bool(profit) else np.nan,
                "PROFIT_OWNER":profit_owner[0] if bool(profit_owner) else np.nan,
                "BOOK_VALUE":book_value[0] if bool(book_value) else np.nan,
                "BOOK_VALUE_OWNER":book_value_owner[0] if bool(book_value_owner) else np.nan,

            }


            # print(ret)
            return ret

        except Exception as e:

            print(e)
            print(df_is[['name', 'code']])
            print(df_fs[['name', 'code']])
            return None

        # return {"finantial_statement": df_fs,
        #         "income_statement": df_is,
        #         "cash_flows": df_cf}

        '''

        TODO: 
        == 1) IDX 참조 안됨, 컬럼순서가 바뀔때가 있음
        == 2) nan값이 응답으로 오는 경우가 있으므로 정상적으로 결과를 받을때까지 while문 처리 해줘야함
        == 3) 상장주식수를 분,반, 사업보고서에서 해당시점껄로 가져오는 방법 마련 (없음)
        4) epsc를 다트에서 주는거 말고 직접 계산 할 수 있게 백데이터를 확보
        5) 사업보고서일때 마지막 분기 데이터만 추려내는 방법 고안 필요

        '''

        '''

        
        TODO: 
        1) IDX 참조 안됨, 컬럼순서가 바뀔때가 있음
        2) nan값이 응답으로 오는 경우가 있으므로 정상적으로 결과를 받을때까지 while문 처리 해줘야함
        3) 상장주식수를 분,반, 사업보고서에서 해당시점껄로 가져오는 방법 마련
        4) epsc를 다트에서 주는거 말고 직접 계산 할 수 있게 백데이터를 확보
        5) 사업보고서일때 마지막 분기 데이터만 추려내는 방법 고안 필요
        


        {'year': '2020', 'target_quarter': '1Q', 'repot_type': '분기보고서', 'epsc': 168.0, 'epsi': 155.0, 'bps_owner': 5507.219, 'bps_total': 10098.286, 'roe_owner': 0.057, 'roe_total': 0.031}
        {'year': '2020', 'target_quarter': '2Q', 'repot_type': '반기보고서', 'epsc': 442.0, 'epsi': 229.0, 'bps_owner': 6026.677, 'bps_total': 11820.945, 'roe_owner': 0.14, 'roe_total': 0.071}
        {'year': '2020', 'target_quarter': '3Q', 'repot_type': '분기보고서', 'epsc': 573.0, 'epsi': 297.0, 'bps_owner': 6154.374, 'bps_total': 12154.977, 'roe_owner': 0.185, 'roe_total': 0.094}
        {'year': '2020', 'target_quarter': '4Q', 'repot_type': '사업보고서', 'epsc': 935.0, 'epsi': 365.0, 'bps_owner': 6528.386, 'bps_total': 12796.075, 'roe_owner': 0.269, 'roe_total': 0.137}

        {'year': '2020', 'target_quarter': '1Q', 'repot_type': '분기보고서', 'epsc': 168.0, 'epsi': 155.0, 'bps_owner': 5507.219, 'bps_total': 10098.286, 'roe_owner': 0.057, 'roe_total': 0.031}
        {'year': '2020', 'target_quarter': '2Q', 'repot_type': '반기보고서', 'epsc': 442.0, 'epsi': 229.0, 'bps_owner': 6026.677, 'bps_total': 11820.945, 'roe_owner': 0.14, 'roe_total': 0.071}
        {'year': '2020', 'target_quarter': '3Q', 'repot_type': '분기보고서', 'epsc': 573.0, 'epsi': 297.0, 'bps_owner': 6154.374, 'bps_total': 12154.977, 'roe_owner': 0.185, 'roe_total': 0.094}
        {'year': '2020', 'target_quarter': '4Q', 'repot_type': '사업보고서', 'epsc': 935.0, 'epsi': 365.0, 'bps_owner': 6528.386, 'bps_total': 12796.075, 'roe_owner': 0.269, 'roe_total': 0.137}

        {'year': '2020', 'target_quarter': '1Q', 'repot_type': '분기보고서', 'epsc': 168.0, 'epsi': 155.0, 'bps_owner': 5507.219, 'bps_total': 10098.286, 'roe_owner': 0.057, 'roe_total': 0.031}
        {'year': '2020', 'target_quarter': '2Q', 'repot_type': '반기보고서', 'epsc': 442.0, 'epsi': 229.0, 'bps_owner': 6026.677, 'bps_total': 11820.945, 'roe_owner': 0.14, 'roe_total': 0.071}
        {'year': '2020', 'target_quarter': '3Q', 'repot_type': '분기보고서', 'epsc': 573.0, 'epsi': 297.0, 'bps_owner': 6154.374, 'bps_total': 12154.977, 'roe_owner': 0.185, 'roe_total': 0.094}
        {'year': '2020', 'target_quarter': '4Q', 'repot_type': '사업보고서', 'epsc': 935.0, 'epsi': 365.0, 'bps_owner': 6528.386, 'bps_total': 12796.075, 'roe_owner': 0.269, 'roe_total': 0.137}

        '''

        ## 여기서 부터 필요한 내용만 추출해보자

        # EPS = df_is



quarter_dic = {
    '1Q': dict(month='03', type='분기보고서', due_date='05-15', from_date="01-01", to_date="03-31", reffering_from_date="05-15", reffering_to_date="08-14"),
    '2Q': dict(month='06', type='반기보고서', due_date='08-14', from_date="04-01", to_date="06-30", reffering_from_date="08-14", reffering_to_date="11-15"),
    '3Q': dict(month='09', type='분기보고서', due_date='11-15', from_date="07-01", to_date="09-30", reffering_from_date="11-15", reffering_to_date="03-31"),
    '4Q': dict(month='12', type='사업보고서', due_date='03-31', from_date="10-01", to_date="12-31", reffering_from_date="03-31", reffering_to_date="05-15"),
}



def get_stockcodes():

    stockcodes = db.selectSingleColumn('''
    
    SELECT STOCKCODE
    FROM
    (
        SELECT STOCKCODE, ROW_NUMBER() OVER (ORDER BY ROE DESC) AS RN
        FROM jazzdb.T_STOCK_FINAN
        WHERE DATE = '2103'
        AND TYPE='C'
        ORDER BY ROE DESC
    ) A
    
    WHERE 1=1
    AND STOCKCODE NOT IN (
    
		SELECT DISTINCT STOCKCODE 
		FROM jazzdb.T_STOCK_FINAN_DART
		WHERE QUARTER IN ('2103', '2106')
    
    )
    

    
    ''')

    return stockcodes





#
if __name__=="__main__":

    # for i, stockcode in enumerate(['079940','079940','079940','079940']):
    # import time
    # time.sleep(60*20)

    dart.set_api_key(api_key=api_key)  # 인증 설정

    # 모든 상장된 기업 리스트 불러오기
    corp_list = dart.get_corp_list()

    for j, stockcode in enumerate(get_stockcodes()):

        start_time = datetime.now()
        ret = []


        try:

            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20170401', to_date_searching='20170515', year='2017', target_quarter='1Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20170601', to_date_searching='20170814', year='2017', target_quarter='2Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20170901', to_date_searching='20171115', year='2017', target_quarter='3Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20180101', to_date_searching='20180415', year='2017', target_quarter='4Q').get_result())

            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20180401', to_date_searching='20180515', year='2018', target_quarter='1Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20180601', to_date_searching='20180814', year='2018', target_quarter='2Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20180901', to_date_searching='20181115', year='2018', target_quarter='3Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20190101', to_date_searching='20190415', year='2018', target_quarter='4Q').get_result())
            #
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20190401', to_date_searching='20190515', year='2019', target_quarter='1Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20190601', to_date_searching='20190814', year='2019', target_quarter='2Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20190901', to_date_searching='20191115', year='2019', target_quarter='3Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20200101', to_date_searching='20200415', year='2019', target_quarter='4Q').get_result())

            ret.append(DartParser(stockcode=stockcode, from_date_searching='20200401', to_date_searching='20200515', year='2020', target_quarter='1Q').get_result())
            ret.append(DartParser(stockcode=stockcode, from_date_searching='20200601', to_date_searching='20200814', year='2020', target_quarter='2Q').get_result())
            ret.append(DartParser(stockcode=stockcode, from_date_searching='20200901', to_date_searching='20201115', year='2020', target_quarter='3Q').get_result())
            ret.append(DartParser(stockcode=stockcode, from_date_searching='20210101', to_date_searching='20210415', year='2020', target_quarter='4Q').get_result())

            ret.append(DartParser(stockcode=stockcode, from_date_searching='20210401', to_date_searching='20210515', year='2021', target_quarter='1Q').get_result())
            ret.append(DartParser(stockcode=stockcode, from_date_searching='20210601', to_date_searching='20210814', year='2021', target_quarter='2Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20210901', to_date_searching='20211115', year='2020', target_quarter='3Q').get_result())
            # ret.append(DartParser(stockcode=stockcode, from_date_searching='20220101', to_date_searching='20220415', year='2020', target_quarter='4Q').get_result())

            ret = [x for x in ret if x is not None]

            df = pd.DataFrame(ret)
            db.insertdf(df, table='jazzdb.T_STOCK_FINAN_DART')
            print(j, stockcode, 'success')

        except Exception as e:
            print(j, stockcode, 'fail')


        '''
          STOCKCODE QUARTER          REVENUE  OPERATE_INCOME          PROFIT    PROFIT_OWNER        BOOK_VALUE  BOOK_VALUE_OWNER
        0    079940    2003 39,068,686,372.0 5,687,604,468.0 4,249,206,526.0 2,177,586,864.0 136,687,207,223.0  74,543,971,368.0
        1    079940    2006 42,170,043,555.0 7,680,824,492.0 7,179,474,158.0 3,545,444,533.0 160,004,572,694.0  81,575,189,832.0
        2    079940    2009 39,644,182,015.0 5,214,977,091.0 4,015,516,346.0 1,704,945,778.0 164,525,928,330.0  83,303,657,536.0
        3    079940    2012 64,097,025,298.0 7,129,577,695.0 4,758,213,788.0             NaN 173,203,629,410.0  88,366,165,232.0

        '''