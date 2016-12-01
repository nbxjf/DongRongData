# coding=utf-8
"""
Author: Austin
Created: 2016-11-18
Version: 1.0
"""
import datetime
import json
import logging.config
import time

import PathResolver
import Gs_Web_EsConvert
import Initer
from common import ETLTools
from common.exceptions import ConvertException
from persist import DbHelper
from persist import EsHelper


def main():
    config_site = DbHelper.fetchmany(stmt='SELECT priority_code, site FROM config_site')
    config_site = dict(zip(map(lambda x: x['site'], config_site), map(lambda x: x['priority_code'], config_site)))

    batch = 10
    site = None
    index = 'drdata_qyk'
    doc_type = 'BusinessInfo'

    '''
        parse_status_success:1
        es_status:2 成功
        es_status:其他 未处理
    '''
    parse_status_success = 1
    es_status_success = 2
    select_company = " SELECT id, company_name FROM %s where parse_status = %s and etl_status !=%s limit %s "

    configs = [
        {'site': 'shanghai_web', 'table': 'gs_shanghai_company', 'finished': False},
        {'site': 'fujian_web', 'table': 'gs_fujian_company', 'finished': False},
        {'site': 'hebei_web', 'table': 'gs_hebei_company', 'finished': False},
        {'site': 'hunan_web', 'table': 'gs_hunan_company', 'finished': False},
        {'site': 'yunnan_web', 'table': 'gs_yunnan_company', 'finished': False},
    ]
    while True:

        if len(filter(lambda x: not x['finished'], configs)) == 0:
            logging.info(" es etl finished, process going to closed")
            break

        for conf in configs:

            if conf['finished']:
                continue

            # items = DbHelper.fetchmany(stmt=select_company, data=(conf['table'], parse_status_success, es_status_success, batch))
            items = DbHelper.fetchmany(stmt=select_company % (conf['table'], parse_status_success, es_status_success, batch))
            if len(items) == 0:
                conf['finished'] = True

            for item in items:
                time_begin = time.time()
                try:
                    handle_single_item(item, config_site=config_site, site=conf['site'], table=conf['table'],
                                       index=index, doc_type=doc_type, es_status_success=es_status_success)
                except ConvertException as e:
                    logging.getLogger().exception(e)
                    raise e
                except Exception as e:
                    logging.getLogger().exception(e)
                    time.sleep(10)
                logging.info('cost: {0:f}'.format(time.time() - time_begin))


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
    value1 = DbHelper.fetchone(select_company_parsed_content, data=(company_id, kwargs['site'], 'gsgs'))['value']
    value2 = DbHelper.fetchone(select_company_parsed_content, data=(company_id, kwargs['site'], 'qygs'))['value']
    converted = Gs_Web_EsConvert.convert(value1, value2)
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

    DbHelper.execute("UPDATE %s set etl_status = %s WHERE id = %s ", (kwargs['table'], kwargs['es_status_success'], company_id))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('es', 'database', 'logger'))
    main()
