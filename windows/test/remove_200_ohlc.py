import common.connector_db as db



codes = db.selectSingleColumn('SELECT STOCKCODE FROM jazzdb.T_STOCK_CODE_MGMT WHERE LISTED=1')


for i, eachcode in enumerate(codes):

    q = 'DELETE FROM jazzdb.T_STOCK_OHLC_MIN WHERE STOCKCODE = "%s" AND DATE = "2020-10-23"'%(eachcode)
    db.delete(q)
    print(i, q)


