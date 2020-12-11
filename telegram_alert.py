import telepot
import os
import sys
import jazzstock_bot.config.config as cf


_TOKEN = cf.TELEBOT_TOKEN
_RECEIVER = cf.TELEBOT_DEBUG
_BOT = telepot.Bot(_TOKEN)

def send_message_telegram(message_dic='TEST'):
    '''

    탤래그램으로 메세지를 직접 보내는 함수

    :param message: dictionary
    :return:
    '''

    print(message_dic)
    _BOT.sendMessage(_RECEIVER, '%s' % (message_dic))
    
if __name__=='__main__':
    
    send_message_telegram(sys.argv[1])