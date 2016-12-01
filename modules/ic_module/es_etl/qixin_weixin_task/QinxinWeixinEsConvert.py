# encoding=utf-8
"""
Author: Jeff
Created: 2016-11-18
Version: 1.0
"""
import json
import sys

if True:
    sys.path.append('../../')
    reload(sys)
    sys.setdefaultencoding('utf-8')
    from common.exceptions import ConvertException
    from common.ETLTools import extract_formatted_date_string, filter_and_trim, get_area_info, extract_and_uniform_fund


def convert(basic_info, change_info):
    try:
        basicinfo = json.loads(basic_info)
        changeinfo = json.loads(change_info)
    except Exception as e:
        return -1
    try:
        # 1 登记信息
        target_reg = get_reg_info(basicinfo)
        # 2 股东信息
        source_data = basicinfo.get('partners', None)
        target_share = map(lambda x: convert_shareholder(x), source_data) if not source_data is None else None
        # 3 主要成员
        source_member = basicinfo.get('employees', None)
        target_member = map(lambda x: convert_member(x), source_member) if not source_member is None else None
        # 4 分支机构
        source_branch = basicinfo.get('branches', None)
        target_branch = map(lambda x: convert_branch(x), source_branch) if not source_branch is None else None
        # 5 变更记录
        source_changelog = changeinfo.get('changerecords', None)
        target_changelog = map(lambda x: convert_changelog(x), source_changelog) if not source_changelog is None else None
        # 6 经营异常
    except ConvertException as ce:
        raise ce
    return {'RegistrationInfo': target_reg, 'ShareholderInfo': target_share, 'MainMember': target_member, 'BranchOrg': target_branch,
        'BusinessInfoChangeLog': target_changelog, 'AbnormalOperation': None}


def get_reg_info(basic_info):
    RegistrationInfo = {}
    RegistrationInfo['LegalPerson'] = basic_info.get('oper_name', None)
    RegistrationInfo['RegisterFund'] = basic_info.get('regist_capi', None)
    RegistrationInfo['_SortRegisterFund'] = extract_and_uniform_fund(RegistrationInfo['RegisterFund']) if not RegistrationInfo[
                                                                                                                  'RegisterFund'] is None else None
    RegistrationInfo['FoundTime'] = extract_formatted_date_string(
        filter_and_trim(basic_info.get('start_date', None))) if not filter_and_trim(basic_info.get('start_date', None)) is None else None
    RegistrationInfo['RegistNo'] = basic_info.get('reg_no', None)
    RegistrationInfo['OrgCode'] = basic_info.get('org_no', None)
    RegistrationInfo['SocialCreditCode'] = basic_info.get('credit_no', None)
    RegistrationInfo['CmpStatus'] = basic_info.get('status', None)
    RegistrationInfo['CmpType'] = basic_info.get('econ_kind', None)
    RegistrationInfo['BusinessScope'] = basic_info.get('scope', None)
    RegistrationInfo['Address'] = basic_info.get('address', None)
    RegistrationInfo['OperatePeriodBegin'] = extract_formatted_date_string(
        filter_and_trim(basic_info.get('term_start', None))) if not filter_and_trim(basic_info.get('term_start', None)) is None else None
    RegistrationInfo['OperatePeriodEnd'] = extract_formatted_date_string(
        filter_and_trim(basic_info.get('term_end', None))) if not filter_and_trim(basic_info.get('term_end', None)) is None else None
    RegistrationInfo['LicenseDate'] = extract_formatted_date_string(
        filter_and_trim(basic_info.get('start_date', None))) if not filter_and_trim(basic_info.get('start_date', None)) is None else None
    RegistrationInfo['RegistAuthority'] = basic_info.get('belong_org', None)
    RegistrationInfo['OriginalName'] = None
    Area = {}
    Area['Name'], Area['Code'] = get_area_info(RegistrationInfo["Address"], RegistrationInfo["RegistAuthority"]) if not (RegistrationInfo[
                                                                                                                             "Address"] or
                                                                                                                         RegistrationInfo[
                                                                                                                             "RegistAuthority"]) is None else None
    RegistrationInfo['Area'] = Area
    RegistrationInfo['Industry'] = None
    return RegistrationInfo


def convert_member(source):
    Member = {'Name': source.get('name'), 'Position': source.get('job_title'), 'Photo': None}
    return Member


def convert_branch(source):
    BranchOrg = {'CompanyId': source.get('eid', None), 'CmpName': source.get('name', None)}
    return BranchOrg


def convert_changelog(source):
    BusinessInfoChangeLog = {'ChangeTime': extract_formatted_date_string(source.get('change_date', None)) if not source.get('change_date',
                                                                                                                            None) is None else None,
                             'ChangeTitle': source.get('change_item', None), 'OldValue': source.get('before_content', None),
                             'NewValue': source.get('after_content', None)}
    return BusinessInfoChangeLog


def convert_shareholder(source):
    should_items = source.get('should_capi_items', None)
    real_items = source.get('real_capi_items', None)
    target_sharehold = {
        'ShareHolder': filter_and_trim(source.get('stock_name', None)) if not source.get('stock_name', None) is None else None,
        'HolderPercent': filter_and_trim(source.get('stock_percent', None)) if not source.get('stock_percent', None) is None else None,
        'HoldingIdentity': filter_and_trim(source.get('stock_type', None)) if not source.get('stock_type', None) is None else None,
        'ShareholderInfoDetails': {'InvestmentTime': should_items[0].get('should_capi_date', None) if not (
            should_items is None or len(should_items) == 0) else None, 'SubscribedCapital': should_items[0].get('shoud_capi', None) if not (
            should_items is None or len(should_items) == 0) else None,
                                   'ActualPaymentTime': real_items[0].get('real_capi_date', None) if not (
                                       real_items is None or len(real_items) == 0) else None,
                                   'ActualCapital': real_items[0].get('real_capi', None) if not (
                                       real_items is None or len(real_items) == 0) else None,
                                   'InvestmentModel': real_items[0].get('invest_type', None) if not (
                                       real_items is None or len(real_items) == 0) else None}}
    return target_sharehold
