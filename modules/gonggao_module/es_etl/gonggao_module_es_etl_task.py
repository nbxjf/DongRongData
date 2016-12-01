# encoding=utf-8
"""
Author: Jeff
Created: 2016-11-17
Version: 1.0
"""
import datetime
import json
import logging.config
import time

from elasticsearch.exceptions import NotFoundError

import GonggaoWebEsConvert
import Initer
import PathResolver
from common.ETLTools import generate_company_id
from common.exceptions import ConvertException
from persist import DbHelper, EsHelper


def main():
    index = 'drdata_qyk'
    doc_type = 'RmfyggMessage'
    site = 'http://rmfygg.court.gov.cn/psca/lgnot/bulletin/page/'
    '''
    es_status 0 未导入
    es_status 1 已导入
    es_status 2 已存在
    '''
    es_status_success = 1
    es_status_exists = 2
    fetch_rows_limit = 1

    stmt = 'select id,old_data,web_data from gonggao_data where es_status != %s and es_status != %s limit %s'
    while True:
        logging.info("round starts")
        items = DbHelper.fetchmany(stmt=stmt, data=(es_status_success, es_status_exists, fetch_rows_limit))
        for item in items:
            time_begin = time.time()
            try:
                handle_single_item(item, index=index, doc_type=doc_type, site=site, es_status_success=es_status_success,
                                   es_status_exists=es_status_exists)
            except ConvertException as ce:
                logging.getLogger().exception(ce)
                raise ce
            except Exception as e:
                logging.getLogger().exception(e)
                time.sleep(60)
            logging.info('cost: {0:f}'.format(time.time() - time_begin))
        logging.info("round ends")


def handle_single_item(item, **kwargs):
    id, old_data, web_data = item['id'], item['old_data'], item['web_data']
    logging.info("starts to process gonggao_dta id: %s" % (id))
    convert_list = GonggaoWebEsConvert.convert(old_data, web_data)
    for convert in convert_list:
        company_id_es = generate_company_id(str(id) + convert['CmpName'])
        status = check_existence(company_id_es, index=kwargs['index'], doc_type=kwargs['doc_type'])
        convert['Meta'] = {'Source': kwargs['site'], 'Time': datetime.datetime.now().date().__str__()}
        if status == 0:  # 不存在
            logging.info("starts to index id : %s,company : %s,doc_id : %s" % (id, convert['CmpName'], company_id_es))
            EsHelper.es.index(index=kwargs['index'], doc_type=kwargs['doc_type'], id=company_id_es,
                              body=json.dumps(convert, ensure_ascii=False, encoding="utf-8"))
            logging.info("inserted into es")
        elif status == 1:  # 存在
            DbHelper.execute("UPDATE gonggao_data set es_status = %s WHERE id = %s ", (kwargs['es_status_exists'], id))
            logging.info("company exists in es")
            return
        DbHelper.execute("UPDATE gonggao_data set es_status = %s WHERE id = %s ", (kwargs['es_status_success'], id))


def check_existence(doc_id, **kwargs):
    try:
        EsHelper.es.get(index=kwargs['index'], doc_type=kwargs['doc_type'], id=doc_id)
    except NotFoundError as e:
        return 0
    return 1


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('es', 'database', 'logger'))
    main()
