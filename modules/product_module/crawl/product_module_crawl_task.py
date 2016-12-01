# -*- encoding:UTF-8 -*-
"""
Author: Austin
Created: 2016-10-27
Version: 1.0
"""

import logging.config
import requests
import time
from lxml import etree
from mysql.connector import IntegrityError

import PathResolver
import Initer
from persist import DbHelper
from common import proxy


# extract data from html page
def parse(page):
    # with open("demo.html", "r") as f:
    #     data = f.readlines()
    # page = "".encode("utf-8").join(data)
    # print page

    html = etree.HTML(page)
    trs = html.xpath("body/div/div[@class='bodydiv']/table[@class='gridBody']/tbody/tr")
    items = []
    for tr in trs:
        tds = tr.xpath("td")
        item = map(lambda x: None if ((x.text is None) or (x.text.strip() == u'')) else x.text.strip(), tds)
        item.remove(item[0])
        items.append(item)
    return items


# crawl/fetch one html, extract data from it, then insert them into database
def process(cookie, pagenum):
    url = 'http://webdata.cqccms.com.cn/webdata/query/CCCCerti.do'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '110',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookie,
        'Host': 'webdata.cqccms.com.cn',
        'Origin': 'http://webdata.cqccms.com.cn',
        'Referer': 'http://webdata.cqccms.com.cn/webdata/query/CCCCerti.do;jsessionid=qxkxYRZYCCtHGGd17y3J5TlsJqNvSGLGTt1hVcpp618JkmTfpp1T!-510284702',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.63 Safari/537.36'
    }
    data = {
        'keyword': u"公司".encode('GBK'),
        '_h_select_chaxuntype': 'appname',
        'chaxuntype': 'appname',
        'pageNumber': pagenum,
        'pageSize': 10,
        'sortColumns': 'null'
    }
    proxies = proxy.get_proxy("products_info")
    crawling_response = requests.post(url=url, data=data, headers=headers, proxies=proxies)
    # crawling_response = requests.post(url=url, data=data, headers=headers)
    if crawling_response.status_code != 200:
        time.sleep(10)
    content = crawling_response.text
    print content

    parsed_content = parse(content)
    if len(parsed_content) == 0:
        logging.getLogger("Crawler").info("Nothing parsed ")
        return 0
    try:
        DbHelper.executemany(
            "INSERT INTO product(certificat_no,applicant,manufacturer,factory,product,model_specification,standard,"
            "issue_date,original_issue_date,expiry_date,`status`,status_changing_time,reason,attachment) VALUES(%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s)", data=parsed_content)
    except IntegrityError as e:
        logging.getLogger("Crawler").exception("Exceptions Occurs During One Batch Process")
        logging.getLogger("Crawler").exception(e)
    except Exception, e:
        logging.getLogger("Crawler").exception("Exceptions Occurs When Inserting Into DB ")
        logging.getLogger("Crawler").exception(e)
        raise e


def main():
    max_page_num = 100000
    cookie = "JSESSIONID=qs1YYRPDnCcXnjvsQMJyyhn4TjT53s3S2xLzGVQ8zpy6jXVnhQnT!-510284702"

    for pagenum in range(1, max_page_num):
        logging.getLogger("Crawler").info("Batch Processing Begins, the PageNum is : %s" % (pagenum,))
        try:
            status = process(cookie=cookie, pagenum=pagenum)
            if status is not None and (status == 0):
                logging.getLogger("Crawler").info("Cookie Invalid or No Items Left to be Crawled, Process Going to Exit ")
                break
        except Exception as e:
            logging.getLogger("Crawler").exception("Exceptions Occurs During One Batch Process")
            logging.getLogger("Crawler").exception(e)
        logging.getLogger("Crawler").info("Batch Processing Ends")
        time.sleep(3)
    logging.getLogger("Crawler").info("Crawling Process Ends")


if __name__ == "__main__":
    PathResolver.resolve()
    Initer.initialize(('logger', 'database', 'proxy'))
    main()
