# -*- coding:utf-8 -*-
"""
Author: Austin
Created: 2016-11-14
Version: 2.0
"""
import logging.config
import time

import PathResolver
from persist import DbHelper, StoreHelper
import Initer


def main():
    site = "tianyancha_web"
    batch = 200
    while True:

        logging.getLogger().info(" Batch begins ")
        stime = time.time()
        sql = '''
            select b.id, b.crawl_time, b.src_detail from (
                select id from tianyancha_web_company where crawl_status = %s and parse_status = %s  limit %s
            ) as a left join (select id, crawl_time, src_detail from tianyancha_web_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (1, 0, batch))
        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            logging.getLogger().info(" begin to parse company-id : %s " % (item[0],))
            data_table = "common_data_all"

            try:
                # parse html page
                company_id, crawl_time, src_detail = item[0], item[1], item[2]
                StoreHelper.store_data(data_table=data_table,
                                       company_id=company_id,
                                       site=site,
                                       crawl_time=crawl_time,
                                       data={u'detail': src_detail})
                logging.getLogger().info(" data stored ")
                # update parse status
                DbHelper.execute("update tianyancha_web_company set parse_status = %s, data_table_name =%s  where id = %s",
                                 (1, data_table, item[0]))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")
            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                DbHelper.execute("update tianyancha_web_company set parse_status = %s  where id = %s ", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
