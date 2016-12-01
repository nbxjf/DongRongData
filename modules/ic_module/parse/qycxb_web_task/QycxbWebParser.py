# coding=utf-8
from lxml import etree
import sys

if True:
    sys.path.append("../../")
    reload(sys)
    from common.exceptions import ParseException


def parse(page):
    try:
        html = etree.HTML(page)
        result = processing(html)
        return result
    except Exception as e:
        raise ParseException(e.message)


def processing(page):
    sections = page.xpath("body/div[@class='wrap-w clf']/article[@class='main']/section[@class='content-dtl']")
    if len(sections) == 0:
        return None

    result = {}

    parts = sections[0].xpath("child::*")

    # parts[0] 基本信息: 主体信息+机构信息
    var_sections = parts[0].xpath("section")
    m = map(lambda x: get_value_from_label_span(x), var_sections[0].xpath("ul/li/span[@class='tit']"))
    n = map(lambda x: get_value_from_label_span(x), var_sections[0].xpath("ul/li/span[@class='info']"))
    result[u"主体信息"] = dict(zip(m, n))  # 主体信息

    if len(var_sections) == 2:
        m = map(lambda x: get_value_from_label_span(x), var_sections[1].xpath("ul/li/span[@class='tit']"))
        n = map(lambda x: get_value_from_label_span(x), var_sections[1].xpath("ul/li/span[@class='info']"))
        result[u"机构信息"] = dict(zip(m, n))

    # parts[1] <!-- 相关搜索 -->
    # parts[2] <!-- 成员信息or股东信息 -->
    lis_memeber = parts[2].xpath("section/div[@class='art-member lock']/ul/li")  # 需要登录
    lis = parts[2].xpath("section/div[@class='art-shareholder']/ul/li")
    holds = []
    for li in lis:
        divs = li.xpath("div/div")
        var = {divs[0].xpath("span")[0].text: divs[0].xpath("ins")[0].text,
               u"详情": map(lambda x: x.text, divs[1].xpath("p"))}
        holds.append(var)
    result[u'股东信息'] = holds

    # parts[3] <!-- 分支机构 -->
    iteration = parts[3].xpath("ul/li/p/a/text()")
    iteration = map(lambda x: x.strip(), filter(lambda x: x is not None, iteration))
    result[u'分支机构'] = iteration

    # parts[4] <!-- 变更信息 -->
    lis_chg = parts[4].xpath("ul/li")
    chgs = []
    for li in lis_chg:
        ems = li.xpath("descendant::em")
        chg = {}
        for em in ems:
            chg[em.text.strip().replace(u"：", u'')] = em.tail.strip() if em.tail is not None else None
        if len(li.xpath("descendant::time")) == 1:
            chg[u"时间"] = li.xpath("descendant::time")[0].text
        chgs.append(chg)
    result[u"变更信息"] = chgs

    # parts[5] <!-- 网页评论 -->
    # parts[6] <!-- 评论框 添加open-pf类名，在手机端展开或者关闭评论框-->
    return result


def get_value_from_label_span(span):
    if len(span.xpath("a")) == 0:
        return None if span.text is None else span.text.strip().replace(u"：", u'').replace("--", '')
    var = [span.text]
    for a in span.xpath("a"):
        var.append(a.text)
        var.append(a.tail)
    result = ''.join(filter(lambda x: x is not None, var))
    return None if result.strip() == '' else result.strip().replace(u"：", u'').replace("--", '')


# test
if __name__ == '__main__':
    with open("test/3.html") as f:
        htmltest = etree.HTML(f.read().decode('utf-8'))
    r = processing(htmltest)
    print r
