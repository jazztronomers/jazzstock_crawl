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
    
def send_image_telegram(image_path):
    _BOT.sendPhoto(_RECEIVER, photo=open(image_path, 'rb'))
    
if __name__=='__main__':
    
    if len(sys.argv) == 1:
        send_image_telegram('profileImage.png')
    else:
        send_message_telegram(sys.argv[1])
