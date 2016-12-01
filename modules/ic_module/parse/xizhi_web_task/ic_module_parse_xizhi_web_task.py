# -*- coding:utf-8 -*-
"""
Author: Austin
Created:
Revised: 2016-11-14
Version: 2.0
"""
import json
import logging.config
import time

import PathResolver
from XiZhiWebHtmlParse import parse
from persist import DbHelper, StoreHelper
import Initer


def main():
    site = "xizhi_web"
    batch = 100
    while True:

        begin = time.time()
        sql = '''
            select b.id, b.update_time, b.data from (
                select id from xizhi_web_company where status = %s and  parse_status = %s limit %s
            ) as a left join (select id, update_time, data from xizhi_web_company ) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (1, 0, batch))

        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            data_table = "common_data_all"
            company_id, crawl_time, data = item['id'], item['update_time'], item['data']
            logging.getLogger().info(" begin to parse company-id : %s " % (company_id,))

            try:
                # parse html page
                detail = parse(data)
                # persist parsed company data into database
                StoreHelper.store_data(data_table=data_table,
                                       company_id=company_id,
                                       site=site,
                                       crawl_time=crawl_time,
                                       data={u'data': json.dumps(detail)})
                logging.getLogger().info(" data stored ")
                # update parse status
                DbHelper.execute("update xizhi_web_company set parse_status = %s, data_table_name =%s  where id = %s",
                                 (3, data_table, company_id))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")
            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                DbHelper.execute("update xizhi_web_company set parse_status = %s  where id = %s ", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - begin))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
