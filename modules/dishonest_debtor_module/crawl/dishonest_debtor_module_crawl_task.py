# -*- encoding:UTF-8 -*-
"""
Author: Austin
Created: 2016-11-03
Version: 1.0
"""

import logging.config
import requests
import time
import json
import re

import PathResolver
import Initer
from persist import DbHelper
from common import proxy


def process():
    keywords_response = requests.get('http://10.51.1.201:3352/getKeywords',
                                     params={"data": json.dumps({"field": "DishonestDebtor", "status": 0}, ensure_ascii=False)})
    if keywords_response.status_code != 200:
        time.sleep(10)
        return
    else:
        companies = json.loads(keywords_response.text)
    logging.getLogger("Crawler").info("Get Companies From ES, And Size is : %s" % (len(companies),))

    for company in companies:
        cmpid, cmpname = company["Id"], company['CmpName']
        logging.getLogger("Crawler").info("Begins to Crawl Info, the CmpName:%s, CmpId:%s " % (cmpname, cmpid))

        url = "https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?resource_id=6899&query=失信被执行人名单&cardNum=" \
              "&iname=%s&areaName=&ie=utf-8&oe=utf-8&format=json&t=1478057603803&cb=jQuery11020955720320494422_1478057574313&_=1478057574315"
        url %= cmpname.encode("utf-8")
        proxies = proxy.get_proxy("Product")

        crawling_response = requests.get(url=url, proxies=proxies)
        content = crawling_response.text

        p = re.compile("/[*][*]/jQuery\d+_\d+\((.*)\)")
        a = p.search(content).group(1)
        b = json.loads(a)
        if b["status"] == 0 & len(b["data"]) != 0:
            c = b["data"][0]["result"]
            d = []
            for i in c:
                e = {
                    "iname": i["iname"],  # 郑州龙跃汽车租赁有限公司
                    "businessEntity": i["businessEntity"],  # 法定代表人或者负责人姓名
                    "courtName": i["courtName"],  # 执行法院
                    "aeraName": i["aeroname"],  # 省份
                    "caseCode": i["caseCode"],  # 案号
                    "duty": i["duty"],  # 生效法律文书确定的义务
                    "performance": i["performance"],  # 被执行人的履行情况
                    "disruptTypeName": i["disruptTypeName"],  # 失信被执行人行为具体情形
                    "publishDateStamp": i["publishDateStamp"]  # 发布时间
                }
                d.append(e)
            DbHelper.execute("INSERT INTO dishonest_debtor(cmpid, parse_content) VALUES(%s, %s)", data=(cmpid, json.dumps(d)))
        else:
            logging.getLogger("Crawler").info("No Dishonest Info Found ")
        notify_response = requests.get(url=url,
                                       params={"data": json.dumps([{"Id": cmpid, "field": "DishonestDebtor", "status": 1}, ], ensure_ascii=False)})

        if notify_response.status_code != 200:
            logging.getLogger("Crawler").info("Action, which notify es, fails.")
        logging.getLogger("Crawler").info("Info Crawled Successfully")
        time.sleep(3)


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


if __name__ == "__main__":
    PathResolver.resolve()
    Initer.initialize(('logger', 'database', 'proxy'))
    main()
