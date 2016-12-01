# coding=utf-8
"""
Author: Jeff
Created: 2016-11-16
Version: 1.0
"""
import json
import sys

if True:
    sys.path.append('../../')
    reload(sys)
    sys.setdefaultencoding('utf-8')
    from common.ETLTools import extract_formatted_date_string, extract_and_uniform_fund, split_operation_period, get_area_info, \
        filter_and_trim
    from common.exceptions import ConvertException


def convert(source):
    root = json.loads(source)
    try:
        # 1 登记信息
        source_registrationinfo = root[u'工商信息']
        target_registrationinfo = business_info_convert(source_registrationinfo) if not (
            source_registrationinfo is None or len(source_registrationinfo) == 0)else None
        # 2 股东信息
        source_shareholds = root[u"股东信息"]
        target_shareholds = map(lambda x: sharehold_convert(x), source_shareholds) if not (
            source_shareholds is None or len(source_shareholds) == 0) else None
        # 3 主要成员
        source_members = root[u"主要人员"]
        target_members = member_convert(source_members) if not (source_members is None or len(source_members) == 0)else None
        # 4 分支机构
        source_branches = root[u'分支机构']
        target_branches = branche_convert(source_branches) if not (source_branches is None or len(source_branches) == 0) else None
        # 5 变更记录
        source_businesschangelogs = root[u'变更人员']
        target_businesschangelogs = map(lambda x: businesschangelog_convert(x), source_businesschangelogs) if not (
            source_businesschangelogs is None or len(source_businesschangelogs) == 0) else None
        # 6 经营异常
    except Exception as e:
        raise ConvertException(e.message)

    return {'RegistrationInfo': target_registrationinfo, 'MainMember': target_members, 'ShareholderInfo': target_shareholds,
            'BranchOrg': target_branches, 'BusinessInfoChangeLog': target_businesschangelogs, 'AbnormalOperation': None}


def branche_convert():
    raise Exception("source_branches not null")


def business_info_convert(source_registration_info):
    registrationinfo = {}
    registrationinfo["CmpType"] = source_registration_info.get(u'公司类型：', None)
    registrationinfo["Address"] = source_registration_info.get(u'企业地址：', None)
    registrationinfo["BusinessScope"] = source_registration_info.get(u'经营范围：', None)
    registrationinfo["Industry"] = None
    registrationinfo["LegalPerson"] = source_registration_info.get(u'法定代表人：', None)
    registrationinfo["SocialCreditCode"] = source_registration_info.get(u'统一社会信用代码：', None)
    registrationinfo["FoundTime"] = extract_formatted_date_string(source_registration_info.get(u'成立日期：', None))
    registrationinfo["LicenseDate"] = extract_formatted_date_string(source_registration_info.get(u'发照日期：', None))
    registrationinfo["CmpStatus"] = source_registration_info.get(u'经营状态：', None)
    registrationinfo["OrgCode"] = source_registration_info.get(u'组织机构代码：', None)
    registrationinfo["RegistNo"] = source_registration_info.get(u'注册号：', None)
    registrationinfo["OriginalName"] = None  # 曾用名
    registrationinfo["RegisterFund"] = source_registration_info.get(u'注册资本：', None)
    registrationinfo["RegistAuthority"] = source_registration_info.get(u'登记机关：', None)
    Area = {}
    Area['Name'], Area['Code'] = get_area_info(registrationinfo["Address"], registrationinfo["RegistAuthority"]) if not (registrationinfo[
                                                                                                                             "Address"] or
                                                                                                                         registrationinfo[
                                                                                                                             "RegistAuthority"]) is None else None
    registrationinfo["Area"] = Area
    registrationinfo["_SortRegisterFund"] = extract_and_uniform_fund(
        source_registration_info.get(u'注册资本：', None)) if not source_registration_info.get(u'注册资本：', None) is None else None
    if source_registration_info.get(u'营业期限：', None) is None:
        registrationinfo['OperatePeriodBegin'], registrationinfo['OperatePeriodEnd'] = None, None
    else:
        registrationinfo['OperatePeriodBegin'], registrationinfo['OperatePeriodEnd'] = split_operation_period(
            source_registration_info.get(u'营业期限：', None))
    return registrationinfo


def businesschangelog_convert(source_businesschangelog):
    target_businesschange = {'ChangeTime': filter_and_trim(source_businesschangelog.get(u'变更日期：', None)),
                             'ChangeTitle': filter_and_trim(source_businesschangelog.get(u'变更项目：', None)),
                             'OldValue': filter_and_trim(source_businesschangelog.get(u'变更前：', None)),
                             'NewValue': filter_and_trim(source_businesschangelog.get(u'变更后：', None))}
    return target_businesschange


def member_convert(source_members):
    target_members = []
    for k, v in source_members.items():
        target_member = {}
        target_member['Name'] = v
        target_member['Position'] = k
        target_member['Photo'] = None
        target_members.append(target_member)
    return target_members


def sharehold_convert(source_sharehold):
    target_sharehold = {
        'ShareHolder': filter_and_trim(source_sharehold.get(u'股东：', None)) if not source_sharehold.get(u'股东：', None) is None else None,
        'HolderPercent': None,
        'HoldingIdentity': filter_and_trim(source_sharehold.get(u'股东类型：', None)) if not source_sharehold.get(u'股东类型：',
                                                                                                             None) is None else None,
        'ShareholderInfoDetails': {'InvestmentTime': None, 'SubscribedCapital': filter_and_trim(
            source_sharehold.get(u'认缴出资(金额/时间):', None)) if not source_sharehold.get(u'认缴出资(金额/时间):', None) is None else None,
                                   'ActualPaymentTime': None, 'ActualCapital': filter_and_trim(
                source_sharehold.get(u'实缴出资(金额/时间)：', None)) if not source_sharehold.get(u'实缴出资(金额/时间)：', None) is None else None,
                                   'InvestmentModel': None}}
    return target_sharehold
