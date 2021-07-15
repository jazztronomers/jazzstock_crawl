import common.connector_db as db
from datetime import datetime


DATE = str(datetime.now().date())

df = db.selectpd('''


SELECT * 
FROM jazzdb.T_STOCK_SHARES_INFO
WHERE 1=1
AND DATE = '%s'
AND HOLDER IN ('발행주식수', '유통주식수')
AND SHARE

'''%(DATE))


pvdf = df.pivot(index='STOCKCODE', columns='HOLDER', values='SHARE').reset_index()

pvdf['CIRCRATE'] = pvdf['유통주식수']/pvdf['발행주식수']
pvdf['DATE'] = DATE
rsdf = pvdf[['STOCKCODE','DATE','CIRCRATE']]


db.insertdf(rsdf, table='jazzdb.T_STOCK_SHARES_CIRCRATE')