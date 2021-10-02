import jazzstock_bot.common.connector_db as db




df = db.selectpd('''

SELECT STOCKCODE, CAST(DATE AS CHAR) AS DATE, OPEN, HIGH, LOW, CLOSE, 0 AS VOLUME, VALUE
FROM jazzdb.T_STOCK_OHLC_DAY
WHERE DATE = '2021-09-24'
''')


db.insertdf(df, 'jazzdb.T_STOCK_OHLC_DAY_CORRECTION')


