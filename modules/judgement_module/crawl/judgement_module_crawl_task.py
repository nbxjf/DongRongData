# -*- encoding:UTF-8 -*-
"""
Author: Austin
Created: 2016-10-27
Version: 1.0
"""

import json
import logging.config
import requests
import time

import PathResolver
import Initer
from persist import DbHelper
from common import proxy


def main():
    batch = 1
    parse_status_success = 1
    crawl_status_success = 1

    while True:
        items = DbHelper.fetchmany("SELECT id, web_data FROM wenshu_web WHERE parse_status != %s limit %s", (parse_status_success, batch))

        for item in items:

            case_list = json.loads(item['web_data'])

            for case in case_list:

                if 'Count' in case.keys():
                    continue
                case_name = case[u'案件名称']

                if not case_name.__contains__(u'公司'):
                    continue

                logging.info('starts to handle id: %s, case id: %s ' % (item['id'], case[u'文书ID']))

                url = 'http://wenshu.court.gov.cn/content/content?DocID=%s' % (case[u'文书ID'],)
                headers = None
                proxies = proxy.get_proxy("WenshuDetail")
                response = requests.get(url=url, headers=headers, proxies=proxies, timeout=15)
                if response.status_code != 200:
                    logging.info('case-fetch fails')
                    time.sleep(10)
                    continue
                content = response.text

                DbHelper.execute("INSERT INTO wenshu_web_detail(doc_id, summary, detail, crawl_status) VALUES (%s, %s, %s, %s)",
                                 (case[u'文书ID'], json.dumps(case), content, crawl_status_success))
                logging.info('case inserted')
                time.sleep(3)

            DbHelper.execute('UPDATE wenshu_web SET parse_status = %s WHERE id = %s ', (parse_status_success, item['id']))


if __name__ == "__main__":
    PathResolver.resolve()
    Initer.initialize(('logger', 'database', 'proxy'))
    main()
