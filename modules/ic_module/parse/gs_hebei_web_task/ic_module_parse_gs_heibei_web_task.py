# -*- coding:utf-8 -*-
# import sys
import json
import logging.config
import time
from lxml import etree

import PathResolver
from persist import StoreHelper, DbHelper
from common.IcParse.IcWebParser import IcWebParser
import Initer  # 解析工商WEB


def main():
    site = 'hebei_web'
    batch = 100
    data_table = 'common_data_all'
    while True:

        stime = time.time()
        sql = '''
            select b.id, b.update_time, b.src_gsgs, b.src_qygs, b.src_bmgs, b.src_sfgs from (
                select id from gs_hebei_company where status = %s and parse_status = %s  limit %s
            ) as a left join (select id, update_time, src_gsgs, src_qygs, src_bmgs, src_sfgs from gs_hebei_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (15, 0, batch))

        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            company_id, update_time = item['id'], item['update_time']
            gsgs, qygs, bmgs, sfgs = item['src_gsgs'], item['src_qygs'], item['src_bmgs'], item['src_sfgs']
            try:
                logging.getLogger().info(" begin to parse company-id : %s " % (company_id,))

                result = IcWebParser().parsing([etree.HTML(text=gsgs),
                                                etree.HTML(text=qygs),
                                                etree.HTML(text=bmgs),
                                                etree.HTML(text=sfgs)])
                StoreHelper.store_data(data_table=data_table,
                                       company_id=company_id,
                                       site=site,
                                       crawl_time=item[1],
                                       data={u'gsgs': json.dumps(result[u'工商公示信息']),
                                             u'qygs': json.dumps(result[u'企业公示信息']),
                                             u'bmgs': json.dumps(result[u'其他部门公示信息']),
                                             u'sfgs': json.dumps(result[u'司法协助公示信息'])})
                logging.getLogger().info(" data inserted ")
                '''
                更新相关信息, 即parse_status 和 data_table_name
                '''
                DbHelper.execute("update gs_hebei_company set parse_status = %s, data_table_name= %s where id = %s",
                                 (1, data_table, company_id))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")
            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                DbHelper.execute("update gs_hebei_company set parse_status = %s where id = %s", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
