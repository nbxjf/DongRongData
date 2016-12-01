# coding=utf-8
import io
import json

from elasticsearch import Elasticsearch, NotFoundError
import ConfigParser

es = None


def init(filename):
    global es
    config_parser = ConfigParser.ConfigParser()
    config_parser.readfp(io.open(filename, encoding="utf8"))
    hosts = json.loads(config_parser.get("default", "hosts"))
    es = Elasticsearch(hosts=hosts)


def check(index, doc_type, doc_id, config_site, site):
    # status
    # 0       数据不存在
    # 1       数据存在，需要更新
    # 2       数据存在，不需要更新

    try:
        response = es.get(index=index, doc_type=doc_type, id=doc_id)
    except NotFoundError:
        return 0
    site_es = response['_source']['Meta']['Source']
    if config_site[site] > config_site[site_es]:
        return 1
    else:
        return 2
