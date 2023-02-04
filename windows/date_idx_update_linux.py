from jazzstock_bot.common import connector_db as db

queryt = '''

DELETE FROM jazzdb.T_DATE_INDEXED;


'''

querye = '''

INSERT INTO jazzdb.T_DATE_INDEXED 
(
	SELECT A.DATE, @ROWNUM := @ROWNUM+1 AS CNT
	FROM
	(
		SELECT DATE FROM jazzdb.T_STOCK_OHLC_DAY
		GROUP BY DATE
		ORDER BY DATE DESC

	) A , (SELECT @ROWNUM := -1) R
);
'''
db.insert(queryt)
db.insert(querye)
