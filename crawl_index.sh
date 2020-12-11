echo " * CRAWL_INDEX"
timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_index.py
echo "EXIT CODE: $?"
echo " * CRAWL_INDEX DONE"
