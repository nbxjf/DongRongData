# -*- coding:utf-8 -*-
import re
from Utils import Utils


class Parser:
    def __init__(self):
        pass

    def parsing(self, page):
        pass


class IcWebParser(Parser):
    def __init__(self):
        self.icModuleParse = IcModuleParser()
        self.enterpriseModuleParser = EnterpriseModuleParser()
        self.otherModuleParser = OtherModuleParser()
        self.judicialAssistanceModuleParser = JudicialAssistanceModuleParser()

    def parsing(self, pages):
        n1 = self.icModuleParse.parsing(pages[0])
        n2 = self.enterpriseModuleParser.parsing(pages[1])
        n3 = self.otherModuleParser.parsing(pages[2])
        n4 = self.judicialAssistanceModuleParser.parsing(pages[3])
        return {u"工商公示信息": n1, u"企业公示信息": n2, u"其他部门公示信息": n3, u"司法协助公示信息": n4}


class IcModuleParser(Parser):
    def __init__(self):
        pass

    def parsing(self, page):
        return {u'登记信息': IcModuleParser.parsing_part1(page),
                u'备案信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_02']"),
                u'股权出质登记信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_03']"),
                u'动产抵押登记信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_04']"),
                u'经营异常信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_05']"),
                u'严重违法信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_06']"),
                u'行政处罚信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_07']"),
                u'抽查检查信息': Utils.parsing_part(
                    page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_08']"),
                u'享受扶持信息': None, u'电子营业执照': None}

    @staticmethod
    def parsing_part1(page):
        r = {}
        tables = page.xpath("//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-01_01']/table")
        for i in tables:
            t = i.xpath("tr/th")
            if t[0].text == u'基本信息':
                p = i.xpath("tr/child::*")
                c = {}
                for j in range(1, len(p)):
                    if j % 2 == 1:
                        pass
                    else:
                        c[p[j - 1].text] = p[j].text
                r[t[0].text.strip()] = c
            else:
                ths = i.xpath("tr")[1].xpath("th")
                p = i.xpath("tr/td")
                r[t[0].text.strip()] = Utils.to_mapped_list(data=p, indexes=Utils.get_text_list(ths))
        return r


class EnterpriseModuleParser(Parser):
    def __init__(self):
        pass

    def parsing(self, page):
        result = {u'企业年报': Utils.parsing_part(
            page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_01']"),
            u'行政许可信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_02']"),
            u'知识产权出质登记信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_03']"),
            u'股东及出资信息': EnterpriseModuleParser.parsing_part_04(
                page, "//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_04']"),
            u'行政处罚信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_05']"),
            u'股权变更信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_06']"),
            u'简易注销信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-02_07']")
        }

        return result

    @staticmethod
    def parsing_part_04(page, url):
        r = {}
        a = page.xpath(url)
        # 股东及出资出资信息
        # JS解析
        if len(a) == 0:
            return None

        def fm(regex_str, string):
            pattern_ = re.compile(regex_str)
            list_temp_ = pattern_.findall(string)
            if len(list_temp_) == 0:
                return None
            else:

                o1 = re.compile(u'(".+")|(\'.+\')').findall(list_temp_[0])

                o2 = o1[0][0].replace('\"', '').replace('\'', '')

                if (o2 is None) or (len(o2.strip()) == 0) or (o2.strip() == u''):
                    return o1[0][1].replace('\"', '').replace('\'', '')
                else:
                    return o2

        def fn(u):
            if (u is None) or (u == ''):
                return None
            else:
                if u == u'1':
                    u = u'货币'
                elif u == u'2':
                    u = u'实物'
                elif u == u'3':
                    u = u'知识产权'
                elif u == u'4':
                    u = u'债权'
                elif u == u'5':
                    u = u'高新技术成果'
                elif u == u'6':
                    u = u'土地使用权'
                elif u == u'7':
                    u = u'股权'
                elif u == u'8':
                    u = u'劳务'
                elif u == u'9':
                    u = u'其他'
                return u

        script = a[0].xpath("script")[0].text
        splits = script.split("list.push(investor);")
        js_parse_result = []
        for i in range(0, len(splits) - 1):
            m1, m2, m3, m4, m5, m6, m7 = None, None, None, None, None, None, None

            m1 = fm(u'investor\.inv =.*;', splits[i])  # 股东
            m2 = fn(fm(u'invt\.conForm.*;', splits[i]))  # 认缴出资方式
            m3 = fm(u'invt\.subConAm.*;', splits[i])  # 认缴出资额（万元）
            m4 = fm(u'invt\.conDate.*;', splits[i])  # 认缴出资日期
            m5 = fn(fm(u'invtActl\.conForm.*;', splits[i]))  # 实缴出资方式
            m6 = fm(u'invtActl\.acConAm.*;', splits[i])  # 实缴出资额（万元）
            m7 = fm(u'invtActl\.conDate.*;', splits[i])  # 实缴出资日期
            y = {u"股东": m1, u'认缴额（万元）': m3, u'实缴额（万元）': m6,
                 u'认缴明细': {u'认缴出资方式': m2, u'认缴出资额（万元）': m3, u'认缴出资日期': m4},
                 u'实缴明细': {u'实缴出资方式': m5, u'实缴出资额（万元）': m6, u'实缴出资日期': m7}}
            js_parse_result.append(y)

        r[u'股东及出资信息（币种与注册资本一致）'] = js_parse_result
        # 变更信息
        n = a[0].xpath("table")[1]
        b = n.xpath("tr")
        p = n.xpath('tr/td')
        r[b[0].xpath("th")[0].text] = Utils.to_mapped_list(p, Utils.get_text_list(b[1].xpath('th')))

        return r


class OtherModuleParser(Parser):
    def __init__(self):
        pass

    def parsing(self, page):
        result = {u'行政许可信息': Utils.parsing_part(
            page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-03_01']"),
            u'行政处罚信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-03_02']")}
        return result


class JudicialAssistanceModuleParser(Parser):
    def __init__(self):
        pass

    def parsing(self, page):
        result = {u'股权冻结信息': Utils.parsing_part(
            page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-06_01']"),
            u'股权变更信息': Utils.parsing_part(
                page, url="//div[@class='cont-r']/div[@class='cont-r-b']//div[@rel='layout-06_02']")}
        return result
