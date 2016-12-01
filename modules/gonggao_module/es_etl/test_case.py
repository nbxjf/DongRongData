# encoding=utf-8
import json

import GonggaoWebEsConvert

if __name__ == '__main__':
    old_data = u''''''
    web_data = u''''''
    res = GonggaoWebEsConvert.convert(old_data, web_data)
    print json.dumps(res)
