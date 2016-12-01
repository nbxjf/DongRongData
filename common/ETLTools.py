# encoding=utf-8
import json
import re
import datetime
import sys
import hashlib

import requests
from exceptions import *

reload(sys)
sys.setdefaultencoding('utf8')


def identify_code(code):
    if re.search(r'-', code.strip(), re.I) and len(code.strip()) == 10:
        return 'organization_code'
    elif re.search(ur'[\u4e00-\u9fa5]+', code.strip().decode('utf-8'), re.I):
        return 'register_code'
    elif len(code.strip()) == 18:
        return 'uniform_code'
    elif len(code.strip()) in (13, 14, 15):
        return 'register_code'
    else:
        return 'unknown_code'


def generate_company_id(source, algorithm="md5"):
    prefix = 'drdata'
    algorithm = hashlib.new(algorithm)
    algorithm.update(prefix + source)
    return algorithm.hexdigest()


def filter_and_trim(source):
    result = source.strip().replace(u'-', '').replace(u'暂无', '')
    if result == '':
        return None
    return result


def extract_formatted_date_string(data_string):
    if isinstance(data_string, long):
        return datetime.datetime.fromtimestamp(data_string / 1000).date().__str__()
    if re.match(u'\d{4}年\d+月\d+日', data_string) is not None:  # 2013年8月14日
        return datetime.datetime.strptime(data_string, u'%Y年%m月%d日').date().__str__()
    if re.match(u'\d{4}年\d+月\d+号', data_string) is not None:  # 2013年8月14日
        return datetime.datetime.strptime(data_string, u'%Y年%m月%d号').date().__str__()
    if re.match(u'\d{4}-\d+-\d+', data_string) is not None:  # 2013-8-14
        matched = re.match(u'\d{4}-\d+-\d+', data_string).group()
        return datetime.datetime.strptime(matched, u'%Y-%m-%d').date().__str__()
    if re.match(u'\d{8}', data_string) is not None:  # 20130814
        return data_string[0: 4] + '-' + data_string[4: 6] + '-' + data_string[6:8]
    if data_string.__contains__(u','):
        data_string = data_string.replace(u'AM', u'').replace(u'PM', u'').strip()
        return datetime.datetime.strptime(data_string, u'%b %d, %Y %H:%M:%S').date().__str__()
    if data_string.__contains__(u'T'):
        return datetime.datetime.strptime(data_string, u'%Y-%m-%dT%H:%M:%S').date().__str__()

    if re.search('\d+', data_string) is not None:
        raise UnIdentifiedDateTimeException('data string not identified')
    return None


def get_area_info(*data):
    response = requests.get('http://10.51.1.201:3351/calAreacode', params={"data": json.dumps(data, ensure_ascii=False)})
    # response = requests.get('http://10.51.0.193:3351/calAreacode', params={"data": json.dumps(data, ensure_ascii=False)})
    r = json.loads(response.text)
    if r['status']:
        return r['name'], r['code']
    else:
        return None, None


def split_operation_period(operation_period):
    m, n = None, None
    if operation_period.__contains__(u'—'):
        splits = operation_period.split(u'—', 2)
    elif operation_period.__contains__(u' - '):
        splits = operation_period.split(u' - ', 2)
    elif operation_period.__contains__(u'至'):
        splits = operation_period.split(u'至', 2)
    else:
        return m, n

    for split in splits:

        t = extract_formatted_date_string(split.strip())
        if splits.index(split) == 0:
            m = t
        else:
            n = t
    return m, n


def extract_and_uniform_fund(fund):
    fund = fund.replace(',', '')
    number = re.search(pattern=r'\d+|\d,+', string=fund).group()
    if number is None:
        return None
    number = float(number)

    # 币种
    rate = 1
    currency_search = re.search(pattern=u'美元|欧元|日元', string=fund)
    if currency_search is not None:
        currency = currency_search.group()
        if currency == u'美元':
            rate = 6.8
        elif currency == u'欧元':
            rate = 7.44
        elif currency == u'日元':
            rate = 0.065

    # 单位
    multiply = 1.0 / 10000
    unit_search = re.search(pattern=u'万|十万|百万|千万', string=fund)
    if unit_search is not None:
        unit = unit_search.group()
        if unit == u'万':
            multiply = 1
        elif unit == u'十万':
            multiply = 10
        elif unit == u'百万':
            multiply = 100
        elif unit == u'千万':
            multiply = 1000
        else:
            pass

    return int(number * multiply * rate)
