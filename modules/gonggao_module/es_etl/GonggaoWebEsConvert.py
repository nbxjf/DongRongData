# encoding=utf-8
"""
Author: Jeff
Created: 2016-11-17
Version: 1.0
"""
import json
import sys
import lxml
from lxml import etree

if True:
    sys.path.append('../../')
    reload(sys)
    sys.setdefaultencoding('utf-8')
    from common.ETLTools import extract_formatted_date_string, generate_company_id
    from common.exceptions import ConvertException


def convert(old_data, web_data):
    old_data = json.loads(old_data)
    json_web_data = data_to_json(web_data)
    try:
        # CmpName
        names = old_data.get('dangshiren').split(u"、")
        # Rmfygg
        Rmfygg = rmfygg_convert(old_data, json_web_data)
        res = []
        for item in names:
            res.append({'CmpName': item, 'CmpId': generate_company_id(item), 'Rmfygg': Rmfygg})
    except Exception as e:
        raise ConvertException(e)
    return res


def rmfygg_convert(old_data, web_data):
    Rmfygg = {'Type': old_data.get('type', None), 'Litigant': old_data.get('dangshiren', None), 'RegNo': old_data.get('no', None),
        'Court': web_data.get('court', None), 'StartDate': extract_formatted_date_string(old_data.get('gonggaoshijian', None)),
        'Message': web_data.get('data', None)}
    return Rmfygg


def data_to_json(source):
    root = lxml.etree.HTML(source)
    notice_data = root.xpath('//div[@class="d22 pt10"]/p/text()')[0].strip()
    notice_court = root.xpath('//div[@class="d23 fr "]/p[1]/text()')[0].strip()
    return {'data': notice_data, 'court': notice_court}
