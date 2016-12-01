# -*- coding:UTF-8 -*-
"""
Author: Austin
Created: 2016-10-26
Version: 1.0
"""
import logging.config
import requests
from urllib import quote
import time
import json
from lxml import etree

import PathResolver
import Initer
from persist import DbHelper
from common import proxy


def parse(page):
    # with open("demo.html", "r") as f:
    #     data = f.readlines()
    # page = "".join(data).decode("utf-8")

    html = etree.HTML(page.decode("utf-8"))
    trs = html.xpath("body/div/table/tr/td/table/tr/td/table/tr/td/form/table[@bordercolor='#CCCCCC']/tr")
    trs.remove(trs[0])
    if len(trs) == 1:
        return []
    else:
        head = trs[0]
        columns = head.xpath("th")
        k = map(lambda x: None if x.text is None else x.text.strip(), columns)
        trs.remove(trs[0])
        result = []
        for tr in trs:
            tds = tr.xpath("td")
            v = map(lambda y: None if y.text is None else y.text.strip(), tds)
            result.append(dict(zip(k, v)))
        return result


def process():
    # get un-crawled company-keyword list
    keywords_response = requests.get('http://10.51.1.201:3352/getKeywords',
                                     params={"data": json.dumps({"field": "SoftwareCopyrightStatus", "status": 0}, ensure_ascii=False)})
    if keywords_response.status_code != 200:
        time.sleep(10)
        return
    else:
        companies = json.loads(keywords_response.text)
    logging.getLogger("Crawler").info("Get Companies From ES, And Size is : %s" % (len(companies),))

    for company in companies:

        cmpid, cmpname = company["Id"], company['CmpName']
        logging.getLogger("Crawler").info("Begins to Crawl Info, the CmpName:%s, CmpId:%s " % (cmpname, cmpid))
        url_template = "http://www.ccopyright.com.cn/cpcc/RRegisterAction.do?method=list&no=fck&sql_name=&sql_regnum=&sql_author=%s&curPage=1"
        url = url_template % (quote(cmpname.replace(u"・", u"").encode("gbk")))
        headers = None
        proxies = proxy.get_proxy("SoftWareCopyright")

        crawling_response = requests.get(url=url, headers=headers, proxies=proxies, timeout=15)
        if crawling_response.status_code != 200:
            time.sleep(10)
            continue
        content = crawling_response.text
        try:
            parsed_content = parse(content)
        except Exception as e:
            logging.getLogger("Crawler").exception("Exceptions occurs when parsing crawled page")
            logging.getLogger("Crawler").exception(e)
            continue
        if len(parsed_content) == 0:
            logging.getLogger("Crawler").info("No software copyright found")
        else:
            DbHelper.execute("INSERT INTO software_copyright(cmpid, src) VALUES(%s, %s)", (cmpid, content))
            parse_status = 1
            DbHelper.execute("UPDATE software_copyright SET parse_status = %s, parsed_content = %s where cmpid = %s ",
                             (parse_status, json.dumps(parsed_content), cmpid))
            logging.getLogger("Crawler").info("Page Parsed Successfully")

        notify_response = requests.get(url=url,
                                       params={"data": json.dumps([{"Id": cmpid, "field": "SoftwareCopyrightStatus", "status": 1}, ], ensure_ascii=False)})

        if notify_response.status_code != 200:
            logging.getLogger("Crawler").info("Action, which notify es, fails.")
        logging.getLogger("Crawler").info("Info Crawled Successfully")
        time.sleep(2)


def main():
    while True:
        logging.getLogger("Crawler").info("Batch Processing Begins")
        try:
            process()
        except Exception, e:
            logging.getLogger("Crawler").exception("Exceptions Occurs During One Batch Process")
            logging.getLogger("Crawler").exception(e)
            pass
        logging.getLogger("Crawler").info("Batch Processing Ends")


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('logger', 'database', 'proxy'))
    main()
