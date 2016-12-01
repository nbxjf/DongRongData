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
    batch = 100
    site = 'qixin_weixin'
    data_table = 'common_data_all'
    while True:

        stime = time.time()
        sql = '''
            select b.id, b.crawl_time, b.src_homepageinfo, b.src_basicinfo, b.src_changeinfo from (
                select id from qixin_weixin_company where crawl_status = %s and parse_status = %s  limit %s
            ) as a left join (select id, crawl_time, src_homepageinfo,src_basicinfo,src_changeinfo from qixin_weixin_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (7, 0, batch))
        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            company_id, crawl_time = item['id'], item['crawl_time']
            pageinfo, basicinfo, changeinfo = item['src_homepageinfo'], item['src_basicinfo'], item['src_changeinfo']
            try:
                logging.getLogger().info(" begin to parse company-id : %s " % (company_id,))

                '''
                1 存储公司信息
                '''
                StoreHelper.store_data(data_table=data_table, company_id=company_id,
                                       site=site,
                                       crawl_time=crawl_time,
                                       data={u'pageinfo': pageinfo, u'basicinfo': basicinfo, u'changeinfo': changeinfo})
                logging.getLogger().info(" data stored ")
                '''
                2 更改状态为已解析以及data-table-name
                '''
                DbHelper.execute("update qixin_weixin_company set parse_status = %s, data_table_name =%s  where id = %s",
                                 (1, data_table, company_id))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")
            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                DbHelper.execute("update qixin_weixin_company set parse_status = %s where id = %s", [2, company_id])
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
