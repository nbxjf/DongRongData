# coding=utf-8
# method test: extract_formatted_date_string

# method test: generate_company_id

from ETLTools import generate_company_id, filter_and_trim, \
    extract_formatted_date_string, get_area_info, split_operation_period, extract_and_uniform_fund, identify_code


def test_generate_company_id():
    company_name = u'重庆金属回收有限责任公司钢城分公司'
    assert generate_company_id(company_name) == '83d7e086a0c97a8ded87d01529555fd0'
    pass


def test_filter_and_trim():
    source = u' 上海 '
    assert filter_and_trim(source) == u'上海'
    assert filter_and_trim(u' 暂无 ') is None
    assert filter_and_trim(u' -') is None
    pass


def test_extract_formatted_date_string():
    assert extract_formatted_date_string(u'2014年2月13日') == '2014-02-13'
    assert extract_formatted_date_string(u'20140213') == '2014-02-13'
    assert extract_formatted_date_string(u'2014-2-13') == '2014-02-13'
    pass


def test_get_area_info():
    a, b = get_area_info(u'辽宁省大连市甘井子区汇善南园21-3号', u'大连市工商行政管理局')
    assert a == u'辽宁省大连市甘井子区'
    assert b is not None
    pass


def test_split_operation_period():
    a = u' 2013年2月3日 - 2014年12月4号'
    r1, r2 = split_operation_period(a)
    assert r1 == '2013-02-03'
    assert r2 == '2014-12-04'

    b = u' 2013年2月3日 至 永久'
    r1, r2 = split_operation_period(b)
    assert r1 == '2013-02-03'
    assert r2 is None

    c = u' 2013-05-07 - 2015-01-10'
    r1, r2 = split_operation_period(c)
    assert r1 == '2013-05-07'
    assert r2 == '2015-01-10'
    pass


def test_extract_and_uniform_fund():
    assert extract_and_uniform_fund(u'301万人民币') == 301
    assert extract_and_uniform_fund(u'10万美元') > 60
    assert extract_and_uniform_fund(u'100000人民币') == 10


def test_identify_code():
    assert identify_code('9131011578240544X8') == 'uniform_code'


if __name__ == '__main__':
    test_identify_code()
    test_extract_and_uniform_fund()
    test_extract_formatted_date_string()
    test_filter_and_trim()
    test_generate_company_id()
    test_get_area_info()
    test_split_operation_period()
