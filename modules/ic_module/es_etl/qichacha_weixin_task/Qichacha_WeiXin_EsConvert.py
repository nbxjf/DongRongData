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
    company = root['result']['Company']
    try:
        target_reg = {}
        source_reg = company

        target_reg['SocialCreditCode'] = source_reg.get('CreditCode', None)
        target_reg['RegistNo'] = source_reg.get('No', None)
        target_reg['OrgCode'] = source_reg.get('OrgNo', None)
        target_reg['CmpType'] = source_reg.get('EconKind', None)
        target_reg['CmpStatus'] = source_reg.get('ShortStatus', None)
        target_reg['LegalPerson'] = source_reg.get('OperName', None)
        target_reg['FoundTime'] = extract_formatted_date_string(source_reg.get('StartDate')) if source_reg.get('StartDate') is not None else None
        target_reg['RegisterFund'] = source_reg.get('RegistCapi', None)
        target_reg['_SortRegisterFund'] = extract_and_uniform_fund(target_reg['RegisterFund']) if target_reg['RegisterFund'] is not None else None
        target_reg['OperatePeriodBegin'] = extract_formatted_date_string(
            source_reg.get('TermStart', None)) if source_reg.get('TermStart', None) is not None else None
        target_reg['OperatePeriodEnd'] = extract_formatted_date_string(
            source_reg.get('TermEnd', None)) if source_reg.get('TermEnd', None) is not None else None

        target_reg['LicenseDate'] = extract_formatted_date_string(source_reg.get('CheckDate')) if source_reg.get('CheckDate') is not None else None
        target_reg['RegistAuthority'] = source_reg.get('BelongOrg', None)
        target_reg['Address'] = source_reg.get(u'Address', None)
        target_reg['BusinessScope'] = source_reg.get(u'Scope', None)

        target_reg['OriginalName'] = None  # 曾用名
        area_parse_result = get_area_info(target_reg['Address'], target_reg['RegistAuthority'])
        target_reg['Area'] = {'Name': area_parse_result[0], 'Code': area_parse_result[1]}
        # target_reg['Industry'] = {'Name': None, 'Code': None}
        target_reg['Industry'] = None

        # 2 股东信息
        source_shareholds = company[u'Partners']
        target_shareholds = map(lambda x: sharehold_convert(x), source_shareholds) if not (source_shareholds is None or len(source_shareholds) == 0)else None

        # 4 分支机构
        source_branches = company[u'Branches']
        target_branches = map(lambda x: {'CmpName': x['Name'], 'CompanyId': generate_company_id(x['Name'])}, source_branches) if not (
            source_branches is None or len(source_branches) == 0)else None

        # 变更信息
        source_chges = company[u'ChangeRecords']
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
    target_sharehold = {'ShareHolder': source_sharehold['StockName'],
                        'HolderPercent': source_sharehold['StockPercent'],
                        'HoldingIdentity': source_sharehold['StockType'],
                        'ShareholderInfoDetails': {
                            'InvestmentTime': source_sharehold['ShoudDate'],
                            'SubscribedCapital': source_sharehold['ShouldCapi'],
                            'ActualPaymentTime': source_sharehold['CapiDate'],
                            'ActualCapital': source_sharehold['RealCapi'],
                            'InvestmentModel': source_sharehold['InvestType']
                        }
                        }
    return target_sharehold


def business_info_change_log(source_chg):
    target_chg = {
        'ChangeTime': source_chg[u'ChangeDate'],
        'ChangeTitle': source_chg[u'ProjectName'],
        'OldValue': source_chg[u'BeforeContent'],
        'NewValue': source_chg[u'AfterContent']
    }
    return target_chg
    pass
