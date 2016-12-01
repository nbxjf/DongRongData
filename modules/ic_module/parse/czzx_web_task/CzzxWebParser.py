# -*- coding:utf-8 -*-
from lxml import etree
import logging.config


def parse(page):
    page = etree.HTML(page)
    a = {u'主干信息': parsing_part0(page),
         u'工商信息': parsing_part1(page),
         u'股东信息': parsing_part2(page, num=0),
         u'主要人员': parsing_part3(page, num=1),
         u'分支机构': parsing_part2(page, num=2),
         u'变更人员': parsing_part2(page, num=3)}
    return a


def parsing_part0(page):
    r = []
    inf = page.xpath("//small[@class='clear m-t-xs text-md text-gray']")
    for i in range(0, len(inf)):
        a = inf[i].xpath("text()")[0]
        try:
            a = a.encode('utf-8').replace("\n", "").replace(" ", "")
        except Exception, err:
            logging.getLogger().info(err)
            logging.getLogger().error()

        if (a is None) or (a == ''):
            continue
        r.append(a)
    inf1 = page.xpath("//div[@class='col-md-4 m-b m-t text-right']/p[@class='m-t text-gray']/text()")
    if inf1 != []:
        st = ''.join(inf1[0]).encode("utf-8").replace("\n", "").replace(" ", "")
        r.append(st)
    return r


def parsing_part1(page):
    r = {}
    lis = page.xpath("//div[@id='base_div']/section[@class='panel b-a base_info']/div[@class='panel-body text-sm']/ul/li")
    for i in range(0, len(lis)):
        c = ' '.join(lis[i].xpath("text()")).encode("utf-8").replace("\n", "").replace(" ", "")
        if c == "" and lis[i].xpath("child::a"):
            c = lis[i].xpath("a")[0].text
            # logging.getLogger().info("c: %s " % c)
            if c != "" and (c is not None):
                c = c.encode("utf-8").replace("\n", "").replace(" ", "")
        b = lis[i].xpath("label")[0].text.encode("utf-8").replace("\n", "").replace(" ", "")
        r[b] = c
    return r


def parsing_part2(page, num):
    div = page.xpath("//div[@class='data_div']/section[@class='panel b-a clear']")[num].xpath("div")
    if len(div) < 3:
        r = []
    else:
        r = []
        d = page.xpath("//div[@class='data_div']/section[@class='panel b-a clear']")[num].xpath("div[@class='col-md-12']")
        for i in range(0, len(d)):
            b = d[i].xpath("section/div/div/div[@class='col-md-6']")
            k = {}
            for j in range(0, len(b)):
                p = ' '.join(b[j].xpath("small/text()")).encode("utf-8").replace("\n", "").replace(" ", "")
                x = ' '.join(b[j].xpath("small/span/text()"))
                if x == '' and b[j].xpath("child::a"):
                    x = b[j].xpath("a")[0].text.encode("utf-8")
                k[p] = x
            r.append(k)
    return r


def parsing_part3(page, num):
    div = page.xpath("//div[@class='data_div']/section[@class='panel b-a clear']")[num].xpath("div")
    if len(div) < 3:
        r = {}
    else:
        r = {}
        d = page.xpath("//div[@class='data_div']/section[@class='panel b-a clear']")[num].xpath("div[@class='col-md-3']")
        for i in range(0, len(d)):
            x = d[i].xpath("section/div/div/a")[0].text.encode("utf-8").replace("\n", "").replace(" ", "")
            p = d[i].xpath("section/div/div/small")[0].text
            r[p] = x
    return r
