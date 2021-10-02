import common.connector_db as db
import pandas as pd
import numpy as np
pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False



def get_stockcodes_all():
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """
            SELECT A.STOCKCODE, A.STOCKNAME
            FROM jazzdb.T_STOCK_CODE_MGMT A
            WHERE 1=1
            AND A.LISTED = 1
            """

    return db.selectpd(query)



def get_stockcodes_xps_xox(quarter):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """
            SELECT STOCKCODE
            FROM jazzdb.T_STOCK_FINAN_XOX
            WHERE 1=1
            AND DATE = '%s'
            """%(quarter)

    return db.selectSingleColumn(query)

def get_xps_xox(stockcode):
    query = '''

    SELECT STOCKCODE, DATE, EPSC, EPSI, BPS
    FROM jazzdb.T_STOCK_FINAN
    WHERE 1=1
    AND STOCKCODE = '%s'
    AND TYPE = 'c'

    '''%(stockcode)

    df_yoy = db.selectpd(query)

    query = '''

    SELECT STOCKCODE, DATE, EPSC, EPSI, BPS
    FROM jazzdb.T_STOCK_FINAN
    WHERE 1=1
    AND STOCKCODE = '%s'
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

        df = df_yoy.merge(df_qoq, on=['STOCKCODE', 'DATE'])

        df = df.replace([np.inf, -np.inf], -999)

        return df[['STOCKCODE', 'DATE'] + column_list]

    return None

current_quarter = '2106'

df_stock = get_stockcodes_all()
stockcodes_crawled = get_stockcodes_xps_xox(quarter=current_quarter)


# ==============================================================
# TEST
# df_xps = get_xps_xox('071460')
# if df_xps is not None and isinstance(df_xps, pd.DataFrame):
#
#     df_xps = df_xps.dropna()
#     df_xps_current_quarter = df_xps[df_xps['DATE']==current_quarter]
#     print(df_xps_current_quarter)
# ==============================================================


for i, stockcode in enumerate(df_stock.STOCKCODE.tolist()):

    if stockcode not in stockcodes_crawled:
        df_xps = get_xps_xox(stockcode)
        if df_xps is not None and isinstance(df_xps, pd.DataFrame):
            df_xps = df_xps.dropna()
            df_xps_current_quarter = df_xps[df_xps['DATE']==current_quarter]
            if len(df_xps_current_quarter) > 0 and df_xps_current_quarter.EPSC_YOY.values[0] != -1:
                db.insertdf(df_xps_current_quarter, 'jazzdb.T_STOCK_FINAN_XOX')
                print(i, stockcode)

