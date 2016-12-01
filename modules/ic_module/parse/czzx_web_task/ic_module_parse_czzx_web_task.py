# -*- coding:utf-8 -*-

import json
import logging.config
import time

import PathResolver
import Initer
from CzzxWebParser import parse
from persist import StoreHelper, DbHelper


def main():
    site = 'czzx_web'
    batch = 100
    data_table = 'common_data_all'
    while True:

        logging.getLogger().info(" Batch begins ")
        stime = time.time()
        sql = '''
            select b.id, b.update_time, b.data, b.data_table_name from (
                select id from chuanzhong_web_company where id >= 809439 and parse_status = %s  limit %s
            ) as a left join (select id, update_time, data, data_table_name from chuanzhong_web_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (0, batch))
        if items is None or len(items) == 0:
            time.sleep(10)
            continue
        for item in items:
            company_id, crawl_time, data = item['id'], item['update_time'], item['data']
            try:

                '''
                存储公司信息
                '''
                result = json.dumps(parse(data), ensure_ascii=False, encoding='utf-8')
                StoreHelper.store_data(data_table=data_table,
                                       company_id=company_id,
                                       site=site,
                                       crawl_time=crawl_time,
                                       data={'基本信息': result})
                logging.getLogger().info(" data inserted ")
                '''
                更新相关信息, 即parse_status 和 data_table_name
                '''
                DbHelper.executemany("update chuanzhong_web_company set parse_status = %s, data_table_name= %s where id = %s",
                                     (1, data_table, company_id))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")
            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (item[0]))
                DbHelper.executemany("update chuanzhong_web_company set parse_status = %s where id = %s", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
