import subprocess
import os
import time
from datetime import datetime
from jazzstock_crawl.windows.crawl_snd_basic import get_stockcode_to_update as snd_counter
from jazzstock_crawl.windows.crawl_ohlc_5min import get_stockcode_to_update as ohlc_min_counter
import warnings
warnings.filterwarnings("ignore")

PATH_CWD = 'c://workspace/jazzstock_crawl/windows'
PATH_LOG = os.path.join(PATH_CWD, 'log/windows_%s.log'%(str(datetime.now().date())))


# 병렬실행 ===============================================================================================
# pa = subprocess.Popen(['python3', '-u', '/workspace/jazzstock_script_runner/child.py', '5'],
#                       stdout=open(os.path.join(PATH_LOG, 'test.log'), 'a'), bufsize=100,
#                       universal_newlines=True)
#
# pb = subprocess.Popen(['python3', '-u', '/workspace/jazzstock_script_runner/child.py', '40'],
#                       stdout=open(os.path.join(PATH_LOG, 'test.log'), 'a'), bufsize=100,
#                       universal_newlines=True)

# dic = {pa.pid: pa,
#        pb.pid: pb}
#
# check_process_is_finish(dic)

# 실행 WITH TIMER ===============================================================================================



def run_process(script_path, iteration=1, life=60):
    i = 0
    while i < iteration:
        i+=1
        start = datetime.now()
        print('* RUN PROCESS %s FOR %s TIME, NOW: %s'%(script_path, i, start))
        process = subprocess.Popen(['python', '-u', script_path],
                              stdout=open(PATH_LOG, 'a'), bufsize=100,
                              universal_newlines=True)


        while True:
            elapesd_seconds = (datetime.now()-start).seconds

            # IF SCRIPT IS DONE
            if process.poll() is not None:
                print(i, process.poll(), elapesd_seconds, 'DONE')
                break

            # IF TIMEOVER
            elif elapesd_seconds > life:
                print(i, process.poll(), elapesd_seconds, "TIMEOVER")
                process.kill()
                break

            # ELAPSED TIME CHECK EVERY 5 SECONDS
            else:
                time.sleep(5)



if __name__=='__main__':

    # snd_flag = 9999
    # the_date = '2020-11-13'
    # while snd_flag > 50:
    #     run_process(script_path=os.path.join(PATH_CWD, 'crawl_snd_basic.py'), iteration=1, life=30)
    #     f = open('c:\\workspace\\jazstock_crawl\\log\\snd.log', 'r')
    #     print(f.readline())
    #     f.close()
    #
    # else:
    #     print("SND_DONE")


    # while ohlc_min_counter('2020-11-13')['result'] > 50:
    #     print(' * COUNTER : %s' % (ohlc_min_counter('2020-11-13')['result']))
    #     run_process(script_path=os.path.join(PATH_CWD, 'date_idx_update.py'), iteration=1, life=60)
    # else:
    #     print("OHLC_DONE")


    # run_process(script_path=os.path.join(PATH_CWD, 'crawl_snd_basic.py'), iteration=1, life=200)


    run_process(script_path=os.path.join(PATH_CWD,'boot.py'),iteration=1,life=60)
    run_process(script_path=os.path.join(PATH_CWD,'crawl_snd_basic.py'),iteration=35,life=200)
    run_process(script_path=os.path.join(PATH_CWD,'date_idx_update.py'),iteration=1,life=60)
    run_process(script_path=os.path.join(PATH_CWD,'crawl_ohlc_5min.py'),iteration=35,life=200)
    run_process(script_path=os.path.join(PATH_CWD,'crawl_index.py'),iteration=2,life=600)



