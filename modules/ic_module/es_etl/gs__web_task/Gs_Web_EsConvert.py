# coding=utf-8
"""
Author: Austin
Created: 2016-11-18
Version: 1.0
"""
import sys

if True:
    sys.path.append('../../')
    reload(sys)
    sys.setdefaultencoding('utf-8')
    from common.ETLTools import *
    from common.exceptions import ConvertException


def convert(gsgs_source, qygs_source):
    root = json.loads(gsgs_source)
    # 1 工商基本信息
    try:
        target_reg = {}
        source_reg = root[u'登记信息'][u'基本信息']

        target_reg['SocialCreditCode'] = filter_and_trim(source_reg.get(u'注册号/')) if identify_code(source_reg.get(u'注册号/')) == u'uniform_code' else None
        target_reg['RegistNo'] = filter_and_trim(source_reg.get(u'注册号/')) if identify_code(source_reg.get(u'注册号/')) == u'register_code' else None
        target_reg['OrgCode'] = filter_and_trim(source_reg.get(u'注册号/')) if identify_code(source_reg.get(u'注册号/')) == u'organization_code' else None
        target_reg['CmpType'] = filter_and_trim(source_reg.get(u'类型')) if source_reg.get(u'类型', None) is not None else None
        target_reg['CmpStatus'] = filter_and_trim(source_reg.get(u'登记状态')) if source_reg.get(u'登记状态', None) is not None else None
        target_reg['LegalPerson'] = filter_and_trim(filter(lambda x: x is not None,
                                                           (source_reg.get(u'法定代表人', None),
                                                            source_reg.get(u'经营者', None),
                                                            source_reg.get(u'负责人', None)))[0])
        target_reg['FoundTime'] = extract_formatted_date_string(filter(
            lambda x: x is not None, (source_reg.get(u'成立日期', None), source_reg.get(u'成立日期', None)))[0])
        target_reg['RegisterFund'] = source_reg.get(u'注册资本', None)
        target_reg['_SortRegisterFund'] = extract_and_uniform_fund(target_reg['RegisterFund']) if target_reg['RegisterFund'] is not None else None
        target_reg['OperatePeriodBegin'] = extract_formatted_date_string(filter(
            lambda x: x is not None, (source_reg.get(u'经营期限自', None), source_reg.get(u'营业期限自', None)))[0])
        target_reg['OperatePeriodEnd'] = extract_formatted_date_string(filter(
            lambda x: x is not None, (source_reg.get(u'经营期限至', None), source_reg.get(u'营业期限至', None)))[0])
        target_reg['LicenseDate'] = target_reg['FoundTime']
        target_reg['RegistAuthority'] = filter_and_trim(source_reg.get(u'登记机关')) if source_reg.get(u'登记机关', None) is not None else None
        target_reg['Address'] = filter_and_trim(filter(lambda x: x is not None,
                                                       (source_reg.get(u'住所', None),
                                                        source_reg.get(u'营业场所', None),
                                                        source_reg.get(u'经营场所', None)))[0])
        target_reg['BusinessScope'] = filter_and_trim(filter(lambda x: x is not None,
                                                             (source_reg.get(u'经营范围', None), source_reg.get(u'业务范围', None)))[0])

        target_reg['OriginalName'] = None  # 曾用名
        area_parse_result = get_area_info(target_reg['Address'], target_reg['RegistAuthority'])
        target_reg['Area'] = {'Name': area_parse_result[0], 'Code': area_parse_result[1]}
        # target_reg['Industry'] = {'Name': None, 'Code': None}
        target_reg['Industry'] = None

        # 2 股东信息
        gsgs_source_shareholds = root[u'备案信息'].get(u'股东信息', None)
        qygs_source_shareholds = json.loads(qygs_source).get(u'股东及出资信息').get(u'股东及出资信息（币种与注册资本一致）', None)
        target_shareholds = map(lambda x: qygs_sharehold_convert(x), qygs_source_shareholds) if not (
            qygs_source_shareholds is None or len(qygs_source_shareholds) == 0)else None
        if target_shareholds is None or len(target_shareholds) == 0:
            target_shareholds = map(lambda x: gsgs_sharehold_convert(x), gsgs_source_shareholds) if not (
                gsgs_source_shareholds is None or len(gsgs_source_shareholds) == 0)else None

        # 3 主要成员
        source_members = root[u'备案信息'].get(u'主要人员信息', None)
        target_members = map(lambda x: member_convert(x), source_members) if not (source_members is None or len(source_members) == 0)else None

        # 4 分支机构
        source_branches = root[u'备案信息'].get(u'分支机构信息', None)
        target_branches = map(lambda x: {'CmpName': x[u'名称'], 'CompanyId': generate_company_id(x[u'名称'])}, source_branches) if not (
            source_branches is None or len(source_branches) == 0)else None

        # 5 变更信息
        source_chges = root[u'登记信息'].get(u'变更信息', None)
        target_chges = map(lambda x: business_info_change_log_convert(x), source_chges) if not (
            source_chges is None or len(source_chges) == 0)else None

        # 6 经营异常
        source_abns = root[u'经营异常信息'].get(u'经营异常信息', None)
        target_abns = map(lambda x: abns_convert(x), source_abns) if not (
            source_chges is None or len(source_chges) == 0)else None

    except Exception as e:
        raise ConvertException(e.message)

    return {
        'RegistrationInfo': target_reg,
        'MainMember': target_members,
        'BranchOrg': target_branches,
        'ShareholderInfo': target_shareholds,
        'BusinessInfoChangeLog': target_chges,
        'AbnormalOperation': target_abns
    }


def member_convert(source_member):
    target_member = {
        'Name': source_member[u'姓名'],
        'Position': source_member[u'职务'],
        'Photo': None
    }

    return target_member


def gsgs_sharehold_convert(source_sharehold):
    target_sharehold = {'ShareHolder': source_sharehold.get(u'股东', None),
                        'HolderPercent': None,
                        'HoldingIdentity': source_sharehold.get(u'股东类型', None),
                        'ShareholderInfoDetails': {
                            'InvestmentTime': None,
                            'SubscribedCapital': None,
                            'ActualPaymentTime': None,
                            'ActualCapital': None,
                            'InvestmentModel': None
                        }
                        }
    return target_sharehold


def qygs_sharehold_convert(source_sharehold):
    target_sharehold = {'ShareHolder': source_sharehold.get(u'股东', None),
                        'HolderPercent': None,
                        'HoldingIdentity': None,
                        'ShareholderInfoDetails': {
                            'InvestmentTime': source_sharehold[u'认缴明细'][u'认缴出资日期'],
                            'SubscribedCapital': source_sharehold[u'认缴明细'][u'认缴出资额（万元）'],
                            'ActualPaymentTime': source_sharehold[u'实缴明细'][u'实缴出资日期'],
                            'ActualCapital': source_sharehold[u'实缴明细'][u'实缴出资额（万元）'],
                            'InvestmentModel': source_sharehold[u'认缴明细'][u'认缴出资方式'],
                        }
                        }
    return target_sharehold


def business_info_change_log_convert(source_chg):
    target_chg = {
        'ChangeTime': source_chg[u'变更日期'],
        'ChangeTitle': source_chg[u'变更事项'],
        'OldValue': source_chg[u'变更前内容'],
        'NewValue': source_chg[u'变更后内容']
    }
    return target_chg


def abns_convert(source_abn):
    target_abn = {
    }
    if source_abn.get(u'移出经营异常名录原因', None) is not None or source_abn.get(u'恢复正常记载状态原因', None):
        target_abn['RemoveOrMoved'] = u'移除'
        target_abn['RemoveOrMovedReason'] = filter_and_trim(filter(lambda x: x is not None,
                                                                   (source_abn.get(u'移出经营异常名录原因', None),
                                                                    source_abn.get(u'恢复正常记载状态原因', None))))
        target_abn['RemoveOrMovedReason'] = filter_and_trim(filter(lambda x: x is not None,
                                                                   (source_abn.get(u'移出日期', None),
                                                                    source_abn.get(u'恢复日期', None))))
        target_abn['RemoveOrMovedReason'] = source_abn.get(u'作出决定机关', None)
    else:
        target_abn['RemoveOrMoved'] = u'加入'
        target_abn['RemoveOrMovedReason'] = filter_and_trim(filter(lambda x: x is not None,
                                                                   (source_abn.get(u'列入经营异常名录原因', None),
                                                                    source_abn.get(u'标记经营异常状态原因', None))))
        target_abn['RemoveOrMovedReason'] = filter_and_trim(filter(lambda x: x is not None,
                                                                   (source_abn.get(u'列入日期', None),
                                                                    source_abn.get(u'标记日期', None))))
        target_abn['RemoveOrMovedReason'] = source_abn.get(u'作出决定机关', None)
    return target_abn
