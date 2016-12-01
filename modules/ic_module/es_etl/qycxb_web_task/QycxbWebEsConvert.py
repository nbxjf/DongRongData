# coding=utf-8
"""
Author: Austin
Created: 2016-11-17
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
    try:
        target_reg = {}
        source_reg = root[u'主体信息']
        source_org = root[u'机构信息']

        target_reg['SocialCreditCode'] = None
        target_reg['RegistNo'] = source_reg.get(u'注册号', None)
        target_reg['OrgCode'] = source_org.get(u'机构代码', None)
        target_reg['CmpType'] = source_reg.get(u'主体类型', None)
        target_reg['CmpStatus'] = source_reg.get(u'经营状态', None)
        target_reg['LegalPerson'] = source_reg.get(u'企业法人', None)
        target_reg['FoundTime'] = extract_formatted_date_string(source_reg.get(u'登记日期')) if source_reg.get(u'登记日期') is not None else None
        target_reg['RegisterFund'] = source_reg.get(u'注册资本', None)
        target_reg['_SortRegisterFund'] = extract_and_uniform_fund(target_reg['RegisterFund']) if target_reg['RegisterFund'] is not None else None
        if source_reg.get(u'经营期限', None) is None:
            target_reg['OperatePeriodBegin'], target_reg['OperatePeriodEnd'] = None, None
        else:
            target_reg['OperatePeriodBegin'], target_reg['OperatePeriodEnd'] = split_operation_period(source_reg.get(u'经营期限'))

        target_reg['LicenseDate'] = extract_formatted_date_string(source_reg.get(u'登记日期')) if source_reg.get(u'登记日期') is not None else None
        target_reg['RegistAuthority'] = source_org.get(u'登记机关', None)
        target_reg['Address'] = source_reg.get(u'企业地址', None)
        target_reg['BusinessScope'] = source_reg.get(u'经营范围', None)

        target_reg['OriginalName'] = None  # 曾用名
        area_parse_result = get_area_info(target_reg['Address'], target_reg['RegistAuthority'])
        target_reg['Area'] = {'Name': area_parse_result[0], 'Code': area_parse_result[1]}
        # target_reg['Industry'] = {'Name': None, 'Code': None}
        target_reg['Industry'] = None

        # 2 股东信息
        source_shareholds = root[u'股东信息']
        target_shareholds = map(lambda x: sharehold_convert(x), source_shareholds) if not (source_shareholds is None or len(source_shareholds) == 0)else None

        # 4 分支机构
        source_branches = root[u'分支机构']
        target_branches = map(lambda x: {'CmpName': x, 'CompanyId': generate_company_id(x)}, source_branches) if not (
            source_branches is None or len(source_branches) == 0)else None

        # 变更信息
        source_chges = root[u'变更信息']
        target_chges = map(lambda x: business_info_change_log(x), source_chges) if not (source_chges is None or len(source_chges) == 0)else None

    except Exception as e:
        raise ConvertException(e.message)

    return {
        'RegistrationInfo': target_reg,
        'MainMember': None,
        'BranchOrg': target_branches,
        'ShareholderInfo': target_shareholds,
        'BusinessInfoChangeLog': target_chges,
        'AbnormalOperation': None
    }


def sharehold_convert(source_sharehold):
    sharehold, holding_identity, subscribed_capital, actual_capital = None, None, None, None
    for k, v in source_sharehold.items():
        if k == u'详情':
            for i in v:
                splits = i.split(u'：', 2)
                if i.__contains__(u'认缴'):
                    subscribed_capital = splits[1]
                elif i.__contains__(u'实缴'):
                    actual_capital = splits[1]
            continue
        holding_identity, sharehold = k, v

    target_sharehold = {'ShareHolder': sharehold,
                        'HolderPercent': None,
                        'HoldingIdentity': holding_identity,
                        'ShareholderInfoDetails': {
                            'InvestmentTime': None,
                            'SubscribedCapital': subscribed_capital,
                            'ActualPaymentTime': None,
                            'ActualCapital': actual_capital,
                            'InvestmentModel': None
                        }
                        }
    return target_sharehold


def business_info_change_log(source_chg):
    target_chg = {
        'ChangeTime': source_chg[u'时间'],
        'ChangeTitle': source_chg[u'变更项目'],
        'OldValue': source_chg[u'变更前'],
        'NewValue': source_chg[u'变更后']
    }
    return target_chg
    pass
