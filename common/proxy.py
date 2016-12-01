import ConfigParser
import io
import json
import logging

import requests

filename = "/"


def init(filepath):
    global filename
    filename = filepath


def get_proxy(site):
    config = ConfigParser.ConfigParser()
    config.readfp(io.open(filename, encoding="utf8"))
    host = json.loads(config.get("default", "proxy_config"))

    proxies = {
        'http': '',
    }

    url = 'http://%s/getproxy?site=%s&' % (host['url'], site)

    try:
        logging.getLogger("Proxy").info("start to get proxyip")
        s = requests.Session()
        r = s.get(url)
        proxies['http'] = json.loads(r.text)['http_proxy']
        logging.getLogger("Proxy").info("suc got proxyip: %s" % str(proxies['http']))
    except Exception, e:
        logging.getLogger("Proxy").error("failed get proxyip")
        logging.getLogger("Proxy").exception(e)
        raise e
    return proxies
