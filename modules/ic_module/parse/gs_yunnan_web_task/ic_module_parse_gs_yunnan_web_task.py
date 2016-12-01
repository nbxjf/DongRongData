# -*- coding:utf-8 -*-
import io
import json
import logging.config
import time
from lxml import etree

import PathResolver
from persist import StoreHelper, DbHelper
from common.IcParse.IcWebParser import IcWebParser
from common.IcParse.Utils import Utils
import Initer


# 解析工商WEB

def main():
    site = 'yunnan_web'
    batch = 100
    data_table = 'common_data_all'
    while True:

        stime = time.time()
        sql = '''
            select b.id, b.update_time, b.src_gsgs, b.src_qygs, b.src_bmgs, b.src_sfgs from (
                select id from gs_yunnan_company where status = %s and parse_status = %s  limit %s
            ) as a left join (select id, update_time, src_gsgs, src_qygs, src_bmgs, src_sfgs from gs_yunnan_company) as b on a.id = b.id
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
                                       crawl_time=update_time,
                                       data={u'gsgs': json.dumps(result[u'工商公示信息']),
                                             u'qygs': json.dumps(result[u'企业公示信息']),
                                             u'bmgs': json.dumps(result[u'其他部门公示信息']),
                                             u'sfgs': json.dumps(result[u'司法协助公示信息'])})
                logging.getLogger().info(" data inserted ")
                '''
                更新相关信息, 即parse_status 和 data_table_name
                '''
                DbHelper.execute("update gs_yunnan_company set parse_status = %s, data_table_name= %s where id = %s",
                                 (1, data_table, company_id))
                logging.getLogger().info(" parse status updated, and data_table_name inserted ")

            except Exception, err:
                logging.getLogger().exception(err)
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                DbHelper.execute("update gs_yunnan_company set parse_status = %s where id = %s", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


# 静态页面测试
def test_case_static_pages():
    # 主（4）页面测试
    with open("pages_test/e01.html") as f:
        page1 = etree.HTML(f.read().decode('utf-8'))
    with open("pages_test/e02.html") as f:
        page2 = etree.HTML(f.read().decode('utf-8'))
    with open("pages_test/e03.html") as f:
        page3 = etree.HTML(f.read().decode('utf-8'))
    with open("pages_test/e04.html") as f:
        page4 = etree.HTML(f.read().decode('utf-8'))

    result = IcWebParser().parsing([page1, page2, page3, page4])
    output = json.dumps(result, ensure_ascii=False)
    f = io.open('output.txt', mode='w', encoding="utf-8")
    f.write(output)
    f.close()
    # 调用年报页面测试
    yearly_report_parsing()


#  年报页面测试
def yearly_report_parsing():
    with open("pages_test/report_paper.html") as f:
        paper = etree.HTML(f.read().decode('utf-8'))
    tables = paper.xpath("/html/body/div[@class='main']/div[@class='notice']/div[@class='cont clearfix']//table")

    result = {}
    for i in range(0, len(tables)):
        if i == 0:
            a = tables[i].xpath("tr/*")
            a.pop(0)
            m = a.pop(0).text
            b = Utils.get_text_list(a)
            n = Utils.list_to_mapped_list(b)[0]
            result[m] = n
        else:
            a = tables[i].xpath("tr")
            b = tables[i].xpath("tr/td")
            if len(a) == 1:
                c = None
            else:
                c = Utils.to_mapped_list(b, Utils.get_text_list(a[1].xpath('th')))
            result[a[0].xpath("th")[0].text] = c


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
