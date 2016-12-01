# coding=utf-8
"""
Author: Austin
Created: 2016-11-15
Version: 1.0
"""
import sys

if True:
    sys.path.append('../../')
    reload(sys)
    sys.setdefaultencoding('utf-8')
    from common.ETLTools import *
    from common.exceptions import ConvertException


def convert(source):
    root = json.loads(source)
    # 1 工商基本信息
    try:
        target_reg = {}
        source_reg = root[u'基本信息'][u'工商基本信息']
        target_reg['SocialCreditCode'] = source_reg.get(u'统一社会信用代码', None)
        target_reg['RegistNo'] = source_reg.get(u'注册号', None)
        target_reg['OrgCode'] = source_reg.get(u'组织机构代码', None)
        target_reg['CmpType'] = source_reg.get(u'公司类型', None)
        target_reg['CmpStatus'] = source_reg.get(u'经营状态', None)
        target_reg['LegalPerson'] = source_reg.get(u'法定代表人', None)
        target_reg['FoundTime'] = extract_formatted_date_string(source_reg.get(u'发照日期')) if source_reg.get(u'发照日期') is not None else None
        target_reg['RegisterFund'] = source_reg.get(u'注册资本', None)
        target_reg['_SortRegisterFund'] = extract_and_uniform_fund(target_reg['RegisterFund']) if target_reg['RegisterFund'] is not None else None
        if source_reg.get(u'营业期限', None) is None:
            target_reg['OperatePeriodBegin'], target_reg['OperatePeriodEnd'] = None, None
        else:
            target_reg['OperatePeriodBegin'], target_reg['OperatePeriodEnd'] = split_operation_period(source_reg.get(u'营业期限'))

        target_reg['LicenseDate'] = extract_formatted_date_string(source_reg.get(u'发照日期')) if source_reg.get(u'发照日期') is not None else None
        target_reg['RegistAuthority'] = source_reg.get(u'登记机关', None)
        target_reg['Address'] = source_reg.get(u'企业地址', None)
        target_reg['BusinessScope'] = source_reg.get(u'经营范围', None)

        target_reg['OriginalName'] = None  # 曾用名
        area_parse_result = get_area_info(target_reg['Address'], target_reg['RegistAuthority'])
        target_reg['Area'] = {'Name': area_parse_result[0], 'Code': area_parse_result[1]}
        # target_reg['Industry'] = {'Name': None, 'Code': None}
        target_reg['Industry'] = None

        # 2 股东信息
        source_shareholds = root[u'基本信息'][u'股东信息']
        target_shareholds = map(lambda x: sharehold_convert(x), source_shareholds) if not (source_shareholds is None or len(source_shareholds) == 0)else None

        # 3 主要成员
        source_members = root[u'基本信息'][u'主要成员']
        target_members = map(lambda x: member_convert(x), source_members) if not (source_members is None or len(source_members) == 0)else None

        # 4 分支机构
        source_branches = root[u'基本信息'][u'分支机构']
        target_branches = map(lambda x: {'CmpName': x, 'CompanyId': generate_company_id(x)}, source_branches) if not (
            source_branches is None or len(source_branches) == 0)else None
    except Exception as e:
        raise ConvertException(e.message)

    return {
        'RegistrationInfo': target_reg,
        'MainMember': target_members,
        'BranchOrg': target_branches,
        'ShareholderInfo': target_shareholds,
        'BusinessInfoChangeLog': None,
        'AbnormalOperation': None
    }


def member_convert(source_member):
    target_member = {}
    for k, v in source_member.items():
        target_member['Name'] = v
        target_member['Position'] = k
        target_member['Photo'] = None
    return target_member


def sharehold_convert(source_sharehold):
    target_sharehold = {'ShareHolder': source_sharehold.get(u'股东信息', None),
                        'HolderPercent': None,
                        'HoldingIdentity': source_sharehold.get(u'股东', None),
                        'ShareholderInfoDetails': {
                            'InvestmentTime': None,
                            'SubscribedCapital': source_sharehold.get(u'认缴出资（金额 / 时间）', None),
                            'ActualPaymentTime': None,
                            'ActualCapital': source_sharehold.get(u'实缴出资（金额 / 时间）', None),
                            'InvestmentModel': None
                        }
                        }
    return target_sharehold
