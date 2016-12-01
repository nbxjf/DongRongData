# m is a string list
class Utils:
    def __init__(self):
        pass

    @staticmethod
    def trim_list(m):
        # m list
        r = []
        for i in range(0, m.__len__()):
            if m[i].strip() != u'':
                m[i] = m[i].replace(u':', u'')
                r.append(m[i])
        return r

    @staticmethod
    def handle(data, keys):
        a, b = {}, []

        key = None
        for i in range(0, data.__len__()):
            if keys.__contains__(data[i]):
                if b.__len__() == 0:
                    key = data[i]
                    pass
                else:
                    a[key] = Utils.list_to_mapped_list(b)
                    b = []
                    key = data[i]
                continue
            b.append(data[i])
            if i == data.__len__() - 1:
                a[key] = Utils.list_to_mapped_list(b)

        return a

    @staticmethod
    # list ---> mapped list
    def list_to_mapped_list(data):
        if data.__len__() == 0:
            return {}
        separate = data[0]
        r = []
        t = {}
        for i in range(0, data.__len__()):
            if data[i] == separate and t.__len__() != 0:
                r.append(t)
                t = {}
            if i % 2 == 0:
                pass
            else:
                t[data[i - 1]] = data[i]
            if i == data.__len__() - 1:
                r.append(t)
        return r

    @staticmethod
    def to_mapped_list(data=[], indexes=[]):
        r = []
        t = {}
        for i in range(0, len(data)):
            mod = i % len(indexes)
            t[indexes[mod]] = data[i].text
            if len(t) == len(indexes):
                r.append(t)
                t = {}
        return r

    @staticmethod
    def get_text_list(m=[]):
        r = []
        for i in m:
            r.append(i.text)
        return r

    @staticmethod
    def parsing_part(page, url):
        a = page.xpath(url)
        r = {}
        for i in range(0, len(a)):
            b = a[i].xpath("table/tr")
            p = a[i].xpath("table/tr/td")
            if len(b) == 1:
                c = None
            else:
                t1 = b[1].xpath('th')
                t2 = Utils.get_text_list(t1)
                t3 = Utils.remove_repetitive_items(t2)
                c = Utils.to_mapped_list(p, t3)
            r[b[0].xpath("th")[0].text] = c

        return r

    @staticmethod
    def remove_repetitive_items(data=[]):
        if (data is None) | (len(data) == 0):
            return []

        tag = data[0]
        r = [tag, ]
        for i in range(1, len(data)):
            if data[i] == tag:
                break
            else:
                r.append(data[i])
        return r