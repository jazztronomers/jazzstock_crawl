import requests
import pandas as pd
import jazzstock_bot.common.connector_db as db
from datetime import datetime as dt

pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False



headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
def crawl_itooza(stockcode):

    url = "https://search.itooza.com/search.htm?seName=%s&cpv=#indexTable3"%(stockcode)
    response = requests.get(url, headers=headers)
    response.encoding='EUC-KR'

    html = response.text

    try:
        df_list = pd.read_html(html)


        ## 연환산

        df = pd.DataFrame(columns=['STOCKCODE', 'TYPE', 'DATE', 'EPSC', 'EPSI', 'PER', 'BPS', 'PBR', 'DV', 'DVR', 'ROE', 'NPR', 'OPR'])


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

        df.DATE = df.DATE.str.replace('월', '', regex=False)
        df.DATE = df.DATE.str.replace('.', '', regex=False)

        df = df[['STOCKCODE', 'DATE', 'TYPE',  'EPSC', 'EPSI', 'PER', 'BPS', 'PBR', 'DV', 'DVR', 'ROE', 'NPR', 'OPR']]

        # df_origin = db.selectpd("SELECT * FROM jazzdb.T_STOCK_FINAN WHERE STOCKCODE = '079940' ORDER BY TYPE ASC, DATE DESC")
        # # print(df_origin)
        # current_quarter = '2103'
        # prev_quarter = '2106'
        # print(df[df['DATE'].isin([current_quarter, prev_quarter])])


        current_quarter = '2106'
        df_current = df[df['DATE'].isin([current_quarter])]

        if len(df_current) > 0 :

            query_delete = 'DELETE FROM jazzdb.T_STOCK_FINAN WHERE STOCKCODE="%s" AND DATE = "%s"'%(stockcode, current_quarter)
            db.delete(query_delete)
            db.insertdf(df[df['DATE'].isin([current_quarter])], 'jazzdb.T_STOCK_FINAN')

            return True

        else:
            return False
        ## IF CHANGED OR NOT EXISTS DO UPDATE 를 구현하도록 !



    except Exception as e:
        print(e)


stockcodes = db.selectSingleColumn('''
SELECT STOCKCODE
FROM jazzdb.T_STOCK_FINAN
WHERE DATE = '2103'
AND TYPE='C'
''')

stockcodes_crawled_already = db.selectSingleColumn('''
SELECT STOCKCODE
FROM jazzdb.T_STOCK_FINAN
WHERE DATE = '2106'
AND TYPE = 'C'
AND ROE != -1
''')


# =====================================================
# 2103에는 있으면서
# 2106에는 데이터가 없는 녀석들만을 수집!
# =====================================================


for i, stockcode in enumerate(stockcodes):
    if stockcode not in stockcodes_crawled_already:
        if crawl_itooza(stockcode):
            print(i, stockcode)
