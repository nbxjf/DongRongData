# -*- coding:utf-8 -*-
import logging.config
import time

import PathResolver
from persist import StoreHelper, DbHelper
import Initer


def main():
    site = 'guangdong_weixin'
    batch = 100
    data_table = 'common_data_all'

    while True:

        stime = time.time()
        sql = '''
            select b.id, b.update_time, b.src_gongshang, b.src_qiye, b.src_other, b.src_judicial from (
                select id from gs_guangdong_company where status = %s and parse_status = %s limit %s
            )as a left join (select id, update_time, src_gongshang, src_qiye, src_other, src_judicial from gs_guangdong_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, [15, 0, batch])
        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            company_id, update_time = item['id'], item['update_time']
            gsgs, qygs, bmgs, sfgs = item['src_gongshang'], item['src_qiye'], item['src_other'], item['src_judicial']
            try:
                logging.getLogger().info(" begin to parse company-id : %s " % (item[0],))

                '''
                1 存储公司信息
                '''
                StoreHelper.store_data(data_table=data_table, company_id=company_id,
                                       site=site,
                                       crawl_time=update_time,
                                       data={u'gsgs': gsgs, u'qygs': qygs, u'bmgs': bmgs, u'sfgs': sfgs})
                logging.getLogger().info(" data stored ")
                '''
                2 更新相关信息, 即parse_status 和 data_table_name
                '''
                DbHelper.execute("update gs_guangdong_company set parse_status = %s, data_table_name =%s  where id = %s", (1, data_table, company_id))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")
            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (item[0]))
                DbHelper.execute("update gs_guangdong_company set parse_status = %s where id = %s", [2, company_id])
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
