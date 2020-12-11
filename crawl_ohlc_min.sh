before_cnt=9999
after_cnt=0

while [ "$after_cnt" != "$before_cnt" ] || [ "$after_cnt" -lt 2200 ] || [ "$exit_code" != "0" ]
do
    before_cnt=`python3 /workspace/jazzstock_crawl/counter.py ohlc_min`
    timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_ohlc_5min.py
    exit_code=$?
    echo "EXIT CODE: $exit_code"
    after_cnt=`python3 /workspace/jazzstock_crawl/counter.py ohlc_min`
    echo "OHLC_MIN: $before_cnt, $after_cnt"
    sleep 2
done
