# coding=utf-8
from lxml import etree


# Param page: string of html page
# return parsed values
def parse(html_string):
    # with open("test.html") as f:
    #     page = etree.HTML(f.read().decode('utf-8'))

    # Head Info
    page = etree.HTML(html_string)
    lis = page.xpath("body/div[@class='container clearfix']/div[@class='detail1-content clearfix']/div[@id='details-wrap']/div/ul/li")
    m, n = [], []
    for li in lis:
        m_var = li.xpath("a")[0].text.strip()
        n_var = None if len(li.xpath("a/span")) == 0 else li.xpath("a/span")[0].text.strip()
        m.append(m_var)
        n.append(n_var)
    result = dict(zip(m, n))
    table = page.xpath("//div[@id='details-content']/div/div")

    '''
    工商基本信息
    '''
    a0 = table[0].xpath("div/table/tbody/tr/td")
    b0 = []
    for i in a0:
        t = get_value_from_label_td(i)
        b0.append(t)
    r0 = trans_list_to_map(b0)

    '''
    股东信息
    '''
    titles = table[1].xpath("div/table/thead/tr/th/text()")
    trs = table[1].xpath("div/table/tbody/tr")
    r1 = []
    for tr in trs:
        tds = tr.xpath("td")
        a1 = []
        for td in tds:
            b1 = get_value_from_label_td(td)
            a1.append(b1)
        c1 = dict(zip(titles, a1))
        r1.append(c1)

    '''
    主要成员
    '''
    a2 = table[2].xpath("ul/li/span/text()")
    r2 = trans_list_to_mapped_list_by_two(a2)

    '''
    分支机构
    '''
    a3 = table[3].xpath("ul/li/span")
    r3 = []
    for a in a3:
        t = a.text.strip()
        r3.append(t)

    result[u"基本信息"] = {u'工商基本信息': r0, u"股东信息": r1, u'主要成员': r2, u'分支机构': r3}
    return result


def get_value_from_label_td(label):
    b = label.xpath("text()|span/text()|a/text()")

    result_text = None
    for j in b:
        if (j is not None) and len(j.strip()) != 0:
            j.strip()
            result_text = j.strip().replace(u"：", "")

    return result_text


def trans_list_to_map(data_list):
    r = {}
    for i in range(0, len(data_list)):
        if i % 2 == 0:
            pass
        else:
            r[data_list[i - 1]] = data_list[i]

    return r


def trans_list_to_mapped_list(data_list, keys):
    if len(data_list) == 0:
        return None
    length = len(keys)
    map_ = None
    result = []
    for i in range(0, length):
        if i % length == 0:
            if not (map_ is None or len(map_) == 0):
                result.append(map_)
            map_ = {}
        map_[keys[i % length]] = data_list[i]
        if i == len(data_list) - 1:
            result.append(map_)
    return result


def trans_list_to_mapped_list_by_two(data_list):
    if len(data_list) == 0:
        return None
    data_list = map(lambda x: x.strip().replace(u"：", ""), data_list)
    map_ = None
    result = []
    for i in range(0, len(data_list)):
        if i % 2 == 0:
            if not (map_ is None or len(map_) == 0):
                result.append(map_)
            map_ = {}
        else:
            map_[data_list[i - 1]] = data_list[i]
        if i == len(data_list) - 1:
            result.append(map_)
    return result


if __name__ == "__main__":
    with open("/test/2.html") as f:
        page = f.read().decode('utf-8')
    m = parse(page)
    print m
