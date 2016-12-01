# coding=utf-8
"""
Author: Austin
Created: 2016-11-15
Version: 1.0
"""
import datetime
import json
import logging.config
import sys
import time

import PathResolver
import Initer
import XiZhiWebEsConvert
from common import ETLTools
from common.exceptions import ConvertException
from persist import DbHelper
from persist import EsHelper

print sys.path


def main():
    retrieved_config_site = DbHelper.fetchmany(stmt='SELECT priority_code, site FROM config_site')
    config_site = dict(zip(map(lambda x: x['site'], retrieved_config_site), map(lambda x: x['priority_code'], retrieved_config_site)))

    batch, site, index, doc_type = 10, 'xizhi_web', 'drdata_qyk', 'BusinessInfo'
    #     parse_status_success:3
    #     es_status:1 成功
    #     es_status:其他 未处理
    parse_status_success, es_status_success = 3, 1
    select_company = " SELECT id, company_name FROM xizhi_web_company where parse_status = %s and es_status !=%s limit %s "

    while True:

        items = DbHelper.fetchmany(stmt=select_company, data=(parse_status_success, es_status_success, batch))

        logging.info('batch begins, the size is %s:' % (len(items),))
        batch_begin_time = time.time()

        if len(items) == 0:
            logging.info("no data on condition found in db")
            break

        for item in items:
            time_begin = time.time()
            try:
                handle_single_item(item, config_site=config_site, site=site,
                                   index=index, doc_type=doc_type, es_status_success=es_status_success)
            except ConvertException as e:
                logging.getLogger().exception(e)
                raise e
            except Exception as e:
                logging.getLogger().exception(e)
                time.sleep(10)
            logging.info('cost: {0:f}'.format(time.time() - time_begin))

        logging.info('batch ends, size is: %s, costs:%s' % (len(items), time.time() - batch_begin_time))


def handle_single_item(item, **kwargs):
    company_id, company_name = item['id'], item['company_name'].strip()
    company_id_es = ETLTools.generate_company_id(company_name)
    logging.info("starts to process company: %s, id: %s, the id of es document is: %s "
                 % (company_name, company_id, company_id_es))
    status = EsHelper.check(index=kwargs['index'], doc_type=kwargs['doc_type'],
                            doc_id=company_id_es, site=kwargs['site'], config_site=kwargs['config_site'])

    if status == 2:  # 数据存在
        logging.info("company exists in es")
        return

    select_company_parsed_content = 'SELECT value from common_data_all  where company_id = %s and site = %s and key_desc = %s '
    value = DbHelper.fetchone(select_company_parsed_content, data=(company_id, kwargs['site'], 'data'))['value']
    converted = XiZhiWebEsConvert.convert(value)
    converted['CmpName'] = company_name.strip()
    converted['Meta'] = {'Source': kwargs['site'], 'Time': datetime.datetime.now().date().__str__()}

    if status == 0:  # es中不存在
        EsHelper.es.index(index=kwargs['index'],
                          doc_type=kwargs['doc_type'],
                          id=company_id_es,
                          body=json.dumps(converted, ensure_ascii=False, encoding="utf-8"))
        logging.info("inserted into es")
    else:  # status == 1  es存在,但是优先级较低,因此更新之
        EsHelper.es.update(index='drdata_qyk', doc_type='BusinessInfo', id=company_id_es,
                           body=json.dumps(converted, ensure_ascii=False, encoding="utf-8"))
        logging.info("updated into es")

    DbHelper.execute("UPDATE xizhi_web_company set es_status = %s WHERE id = %s ", (kwargs['es_status_success'], company_id))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('es', 'database', 'logger'))
    main()
