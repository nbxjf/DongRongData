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
    site = 'qichacha_weixin'
    batch = 100
    data_table = 'common_data_all'
    while True:

        stime = time.time()

        sql = '''
            select b.id, b.update_time, b.data from (
                select id from qichacha_weixin_company where status = %s and parse_status = %s  limit %s
            ) as a left join (select id, update_time, data from qichacha_weixin_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (1, 0, batch))

        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            company_id, update_time, data = item['id'], item['update_time'], item['data']
            try:
                logging.getLogger().info(" begin to parse company-id : %s " % (item[0],))

                '''
                1 存储公司信息
                '''
                StoreHelper.store_data(data_table=data_table, company_id=company_id,
                                       site=site,
                                       crawl_time=update_time,
                                       data={u'detail': data})
                logging.getLogger().info(" data stored ")
                '''
                2 更改状态为已解析以及data-table-name
                '''
                DbHelper.execute("update qichacha_weixin_company set parse_status = %s, data_table_name =%s  where id = %s",
                                 [1, data_table, item[0]])
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")

            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                DbHelper.execute("update qichacha_weixin_company set parse_status = %s where id = %s", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
