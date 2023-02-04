import requests
import time
import telepot
import os
import sys
import common.connector_db as db
import jazzstock_bot.config.config as cf
from datetime import datetime
from lxml import html


class jazzstock_monitoring:

    def __init__(self, date_idx=55):
        '''
        :params: istance_list: 사용할 instance_id 로 구성된 리스트
        '''

        # TELEGRAM ================================================
        self.TOKEN = cf.TELEBOT_TOKEN
        self.RECEIVER = cf.TELEBOT_DEBUG
        self.BOT = telepot.Bot(self.TOKEN)
        # =========================================================

        self.TABLE = ['T_STOCK_OHLC_DAY',
                      'T_STOCK_SHARES_INFO',
                      'T_STOCK_SND_DAY',
                      'T_STOCK_MA',
                      'T_STOCK_BB',
                      'T_STOCK_MC',
                      'T_STOCK_BB_EVENT',
                      'T_STOCK_DAY_SMAR',
                      'T_STOCK_TEXT',
                      'T_STOCK_SND_ANALYSIS_RESULT_TEMP',
                      'T_STOCK_SND_ANALYSIS_LONGTERM']
        # 'T_STOCK_OHLC_MIN']
        self.DATE = db.selectSingleValue('SELECT DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = %s' % (date_idx))
        print(self.DATE)

    def run(self):

        ret_msg = "%s\n"%(self.DATE)
        for eachtable in self.TABLE:
            q = f'''
                SELECT COUNT(DISTINCT STOCKCODE)
                FROM jazzdb.{eachtable}
                WHERE 1=1
                AND DATE = "{self.DATE}"
                '''

            ret = db.selectSingleValue(q)
            ret_msg = ret_msg + '%s \t %s\n'%(ret, eachtable)

        return ret_msg



    def send_message_telegram(self, message_dic='TEST'):
        '''

        탤래그램으로 메세지를 직접 보내는 함수

        :param message: dictionary
        :return:
        '''

        print(message_dic)
        self.BOT.sendMessage(self.RECEIVER, '%s' % (message_dic))


if __name__ == '__main__':

    obj = jazzstock_monitoring(date_idx=0)
    ret_msg = obj.run()
    obj.send_message_telegram(ret_msg)
