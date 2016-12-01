# -*- coding:utf-8 -*-
"""
Author: Austin
Created: 2016-11-14
Version: 2.0
"""

import logging.config
import time

import PathResolver
import Initer
from persist import DbHelper, StoreHelper


def main():
    site = 'xysj_weixin'
    batch = 100
    data_table = 'common_data_all'

    while True:
        stime = time.time()
        items = DbHelper.fetchmany(
            "select id, crawl_time, src_list from xysj_weixin_company where crawl_status = 1 and "
            " parse_status = 0  limit %s ", (batch,))
        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            company_id, crawl_time, src_list = item[0], item[1], item[2]
            try:
                StoreHelper.store_data(data_table=data_table, company_id=company_id,
                                       site=site,
                                       crawl_time=crawl_time,
                                       data={u'info': src_list})
                logging.getLogger().info(" data stored ")
                DbHelper.execute("update xysj_weixin_company set parse_status = %s, data_table_name =%s  where id = %s",
                                 (1, data_table, company_id))
                logging.getLogger().info(" parse status updated ")

            except Exception, err:
                logging.getLogger().exception(err)
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
