# -*- coding:utf-8 -*-
"""
Author: Austin
Created: 2016-11-14
Version: 2.0
"""
import json
import logging.config
import time
from QycxbWebParser import parse

import PathResolver
from common.exceptions import ParseException
from persist import DbHelper
import Initer


def main():
    batch = 100
    while True:

        stime = time.time()
        logging.getLogger().info('Batch begins ')
        sql = '''
            select b.id, b.update_time, b.basic_info from (
                select id from qycxb_web_company where status = %s and parse_status = %s  limit %s
            ) as a left join (select id, update_time, basic_info from qycxb_web_company) as b on a.id = b.id
        '''
        items = DbHelper.fetchmany(sql, (1, 0, batch))

        if items is None or len(items) == 0:
            time.sleep(10)
            continue

        for item in items:
            company_id, update_time, basic_info = item[0], item[1], item[2]
            logging.getLogger().info(" begin to parse company-id : %s " % (company_id,))

            try:
                # parse html page
                detail = parse(basic_info)
                # persist parsed company data into database
                DbHelper.executemany("update qycxb_web_company set parse_status = %s, parsed_content =%s  where id = %s",
                                     (1, json.dumps(detail), company_id))
                logging.getLogger().info(" parse status updated, and parsed content inserted ")
            except (ParseException, Exception) as err:
                logging.getLogger().info("exception/err occurs, company id: %s" % (company_id,))
                logging.getLogger().exception(err)
                DbHelper.executemany("update qycxb_web_company set parse_status = %s  where id = %s ", (2, company_id))
                continue

        logging.getLogger().info(" the round of batch-parsing ends, and totally cost %s. " % (time.time() - stime))


if __name__ == '__main__':
    PathResolver.resolve()
    Initer.initialize(('database', 'logger'))
    main()
