# coding=utf-8
"""
Author: Jeff
Created: 2016-11-18
Version: 1.0
"""
import datetime
import json
import logging.config
import time

import PathResolver
import QinxinWeixinEsConvert

import Initer
from common import ETLTools
from common.exceptions import ConvertException
from persist import DbHelper
from persist import EsHelper


def main():
    config_site = DbHelper.fetchmany(stmt='SELECT priority_code, site FROM config_site')
    config_site = dict(zip(map(lambda x: x['site'], config_site), map(lambda x: x['priority_code'], config_site)))

    batch = 10
    site = 'qixin_weixin'
    index = 'drdata_qyk'
    doc_type = 'BusinessInfo'

    '''
        parse_status_success:3
        es_status_success:11 成功
        es_status_exists: 12 其他 未处理
    '''
    parse_status_success = 1
    es_status_success = 11
    es_status_exists = 12
    select_company = " SELECT id, company_name, src_basicinfo, src_changeinfo FROM qixin_weixin_company where parse_status = %s and etl_status !=%s and etl_status !=%s limit %s "

    while True:

        items = DbHelper.fetchmany(stmt=select_company, data=(parse_status_success, es_status_success, es_status_exists, batch))

        for item in items:
            time_begin = time.time()
            try:
                handle_single_item(item, config_site=config_site, site=site, index=index, doc_type=doc_type,
                                   es_status_success=es_status_success, es_status_exists=es_status_exists)
            except ConvertException as e:
                logging.getLogger().exception(e)
                raise e
            except Exception as e:
                logging.getLogger().exception(e)
                time.sleep(10)
            logging.info('cost: {0:f}'.format(time.time() - time_begin))


def handle_single_item(item, **kwargs):
    company_id, company_name, basic_info, change_info = item['id'], item['company_name'].strip(), item['src_basicinfo'], item[
        'src_changeinfo']
    company_id_es = ETLTools.generate_company_id(company_name)
    logging.info("starts to process company: %s, id: %s, the id of es document is: %s " % (company_name, company_id, company_id_es))
    status = EsHelper.check(index=kwargs['index'], doc_type=kwargs['doc_type'], doc_id=company_id_es, site=kwargs['site'],
                            config_site=kwargs['config_site'])

    if status == 2:  # 数据存在
        DbHelper.execute("UPDATE qixin_weixin_company set etl_status = %s WHERE id = %s ", (kwargs['es_status_exists'], company_id))
        logging.info("company exists in es")
        return

    converted = QinxinWeixinEsConvert.convert(basic_info, change_info)
    if converted == -1:
        DbHelper.execute("UPDATE qixin_weixin_company set etl_status = %s WHERE id = %s ", (kwargs['es_status_exists'], company_id))
        logging.info("data error")
        return
    converted['CmpName'] = company_name
    converted['Meta'] = {'Source': kwargs['site'], 'Time': datetime.datetime.now().date().__str__()}

    if status == 0:  # es中不存在

        EsHelper.es.index(index=kwargs['index'], doc_type=kwargs['doc_type'], id=company_id_es,
                          body=json.dumps(converted, ensure_ascii=False, encoding="utf-8"))
        logging.info("inserted into es")
    else:  # status == 1  es存在,但是优先级较低,因此更新之
        EsHelper.es.update(index='drdata_qyk', doc_type='BusinessInfo', id=company_id_es,
                           body=json.dumps(converted, ensure_ascii=False, encoding="utf-8"))
        logging.info("updated into es")

    DbHelper.execute("UPDATE qixin_weixin_company set etl_status = %s WHERE id = %s ", (kwargs['es_status_success'], company_id))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('es', 'database', 'logger'))
    main()
