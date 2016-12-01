#coding=utf-8
import hashlib
import re
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import json
import logging
import db as db
import time
from datetime import datetime
from elasticsearch import Elasticsearch



def utcstr2local(utc_str):
    UTC_FORMAT = "%Y-%m-%dT%H:%M:%S+08:00"
    return datetime.datetime.strptime(utc_str, UTC_FORMAT)


def getCompany(sql):
    res = db.sql_fetch_rows(sql)
    return res

def getValue(dict_str, key):
    if dict_str.has_key(key):
        if dict_str[key] == [] or dict_str[key] == "" or dict_str[key] == None:
            return None
        return dict_str[key]
    else:
        return None

def subData(data):
    data = re.subn(r'(.*)','',data)
    return data[0]

def getdigest(src, algorithm = "md5"):
    algorithm = hashlib.new(algorithm)
    algorithm.update(src)
    return algorithm.hexdigest()

def getData(index,type,id):
    ss = {}
    try:
        ss = es.get(index=index,doc_type=type,id=id)
    except Exception ,err:
        #logging.getLogger().exception(err)
        return None
    return ss

if __name__ == '__main__':
    logging.config.fileConfig("conf_log.conf")
    # logging.getLogger().info("start main")
    # logging.getLogger().info("start to init mysql connection pool")
    db.sql_connect("default.ini", "spider_con_config")
    # logging.getLogger().info("start thread etl name list for common_data_all site = czzx_web")
    es = Elasticsearch(hosts='10.51.1.203')

    while True:
        logging.getLogger().info("start to get info")
        yearreport_list = getCompany("select id,company_id,year from gs_fujian_yearreport where etl_status = 0  limit 10")
        if yearreport_list == [] :
            logging.getLogger().info("no more company_data found")
        else:
            for item in yearreport_list:
                uid = item[0]
                # print uid
                company_id = item[1]
                company_year = item[2]

                yearreport_detail = getCompany("select id,company_id,site,key_desc,value from common_data_all where company_id = %s and site='fujian_web' and key_desc='%s'" % (company_id,company_year))

                detail_uid = yearreport_detail[0][0]
                company_id = yearreport_detail[0][1]
                site = yearreport_detail[0][2]
                key_desc = yearreport_detail[0][3]
                value = json.loads(yearreport_detail[0][4])
                print value

                ReportBasicInfo = {}   # 基本信息
                ContactInfo = {}  # 联系方式
                ReportShareholderInfo = []      # 股东及出资信息
                WebSiteInfo = []  # 网站/网店信息
                ForeignInvestmentInfo = []   # 对外投资信息
                AssetInfo = {}   # 企业资产状况信息
                ShareholderChangeInfo = []   # 股权变更信息
                ReportChangeInfo = []  # 修改记录
                Meta = {}

                # 担保信息
                guarantee_info = getValue(value,"guarantee-info")
                if guarantee_info != None:
                    raise Exception("担保信息不为空")

                # 网站/网店信息
                web_info = getValue(value,"web-info")
                if web_info != None:
                    raise Exception("网页信息不为空")

                # 股东及出资信息
                shareholder_info = getValue(value,"shareholder-info")
                if shareholder_info != None:
                    for item in range(1,len(shareholder_info)):
                        if(len(shareholder_info[item])< 6):
                            break
                        reportShareholderInfo = {}
                        reportShareholderInfo['ShareHolder'] = shareholder_info[item][0]
                        reportShareholderInfo['InvestmentModel'] = shareholder_info[item][3]
                        reportShareholderInfo['SubscribedCapital'] = shareholder_info[item][1]
                        reportShareholderInfo['InvestmentTime'] = shareholder_info[item][2]
                        reportShareholderInfo['ActualCapital'] = shareholder_info[item][4]
                        reportShareholderInfo['ActualPaymentTime'] = shareholder_info[item][5]
                        ReportShareholderInfo.append(reportShareholderInfo)

                # 股票信息
                stock_info = getValue(value,"stock-info")
                if stock_info != None:
                    raise Exception("stock_info信息不为空")


                # 企业资产状况信息
                assets_info = getValue(value,"assets-info")
                AssetInfo['TotalAsset'] = getValue(assets_info,u"资产总额")  # 资产总额	TotalAsset
                AssetInfo['TotalOwnerEquity'] = getValue(assets_info,u"所有者权益合计")  # 所有者权益合计	TotalOwnerEquity
                AssetInfo['TotalOperateInCome'] = getValue(assets_info,u"营业总收入")  # 营业总收入	TotalOperateInCome
                AssetInfo['TotalProfit'] = getValue(assets_info,u"利润总额")   # 利润总额	TotalProfit
                AssetInfo['MainBussinessInCome'] = getValue(assets_info,u"资产总额")   # 营业总收入中主要业务收入	MainBussinessInCome
                AssetInfo['NetProfit'] = getValue(assets_info,u"净利润")   # 净利润	NetProfit
                AssetInfo['TotalTax'] = getValue(assets_info,u"纳税总额")   # 纳税总额	TotalTax
                AssetInfo['TotalDebt'] = getValue(assets_info,u"负债总额")   # 负债总额	TotalDebt

                #变更信息或者修改记录
                change_info = getValue(value,"change-info")
                if change_info != None:
                    raise Exception("change_info变更信息不为空")

                # 对外投资信息
                investment_info = getValue(value,"investment-info")
                if investment_info != None:
                    for item in range(1,len(investment_info)):
                        foreignInvestmentInfo = {}
                        foreignInvestmentInfo['CmpName'] = investment_info[item][0]
                        foreignInvestmentInfo['CompanyId'] = None
                        foreignInvestmentInfo['RegistNo'] = investment_info[item][1]
                        foreignInvestmentInfo['SocialCreditCode'] = investment_info[item][1]
                        ForeignInvestmentInfo.append(foreignInvestmentInfo)
                    # raise Exception("investment_info投资信息不为空")

                # 基本信息
                basic_info = getValue(value,"basic-info")
                ReportBasicInfo['RegistNo'] = getValue(basic_info,u"注册号/统一社会信用代码")   # 注册号	RegistNo
                ReportBasicInfo['SocialCreditCode'] = getValue(basic_info,u"注册号/统一社会信用代码")   # 社会信用代码	SocialCreditCode
                ReportBasicInfo['CmpName'] = getValue(basic_info,u"企业名称")   # 公司名称	CmpName
                ReportBasicInfo['CmpStatus'] = getValue(basic_info,u"企业经营状态")   # 经营状态	CmpStatus
                ReportBasicInfo['EmpCount'] = getValue(basic_info,u"从业人数")   # 从业人数	EmpCount
                ReportBasicInfo['IsChangedShareholder'] = getValue(basic_info,u"有限责任公司本年度是否发生股东股权转让")   # 本年度是否发生股东股权转让	IsChangedShareholder
                ReportBasicInfo['IsChangedInvestment'] = getValue(basic_info,u"企业是否有投资信息或购买其他公司股权")   # 企业是否有投资或购买其他公司股权	IsChangedInvestment

                # 联系方式
                ContactInfo['Tel'] = getValue(basic_info,u"企业联系电话")   # 企业联系电话	Tel
                ContactInfo['Address'] = getValue(basic_info,u"企业通信地址")   # 企业通讯地址	Address
                ContactInfo['Email'] = getValue(basic_info,u"电子邮箱")   # 电子邮箱	Email

                main_info = getValue(value,"main-info")


                Meta['source'] = site
                Meta['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                CmpAnnualReport = {}
                CmpAnnualReport['Meta'] = Meta
                CmpAnnualReport['CmpName'] = ReportBasicInfo['CmpName']
                CmpAnnualReport['CmpId'] = getdigest("drdata" + ReportBasicInfo['CmpName'])
                CmpAnnualReport['Year'] = key_desc[0:4]


                # ReportBasicInfo 基本信息
                CmpAnnualReport['ReportBasicInfo'] = ReportBasicInfo
                # ContactInfo  联系方式
                CmpAnnualReport['ContactInfo'] = ContactInfo

                # ReportShareholderInfo  股东及出资信息
                if ReportShareholderInfo != []:
                    CmpAnnualReport['ReportShareholderInfo'] = ReportShareholderInfo
                else:
                    CmpAnnualReport['ReportShareholderInfo'] = None

                # WebSiteInfo  网站/网店信息
                if WebSiteInfo != []:
                    CmpAnnualReport['WebSiteInfo'] = WebSiteInfo
                else:
                    CmpAnnualReport['WebSiteInfo'] = None

                # ForeignInvestmentInfo   对外投资信息
                if ForeignInvestmentInfo != []:
                    CmpAnnualReport['ForeignInvestmentInfo'] = ForeignInvestmentInfo
                else:
                    CmpAnnualReport['ForeignInvestmentInfo'] = None

                # AssetInfo  企业资产状况信息
                CmpAnnualReport['AssetInfo'] = AssetInfo

                # ShareholderChangeInfo  股权变更信息
                if ShareholderChangeInfo != []:
                    CmpAnnualReport['ShareholderChangeInfo'] = ShareholderChangeInfo
                else:
                    CmpAnnualReport['ShareholderChangeInfo'] = None

                # ReportChangeInfo  修改记录
                if ReportChangeInfo != []:
                    CmpAnnualReport['ReportChangeInfo'] = ReportChangeInfo
                else:
                    CmpAnnualReport['ReportChangeInfo'] = None

                # print json.dumps(CmpAnnualReport, encoding='utf-8', ensure_ascii=False)

                CmpAnnualReportId = getdigest("drdata" + ReportBasicInfo['CmpName'] + CmpAnnualReport['Year'])

                ss = getData("drdata_syq_test","CmpAnnualReport",CmpAnnualReportId)

                if ss == None:
                    res = es.index(index="drdata_syq_test", doc_type="CmpAnnualReport", id=CmpAnnualReportId,body=json.dumps(CmpAnnualReport, ensure_ascii=False, encoding="utf-8"))
                    db.sql_update("UPDATE `gs_fujian_yearreport` SET `etl_status` = 2 WHERE `id` = %d" % int(uid))
                    logging.getLogger().info("uid : %s" % uid)
                    logging.getLogger().info("company_id : %s" % company_id)
                    logging.getLogger().info("insert succ")
                    logging.getLogger().info("%s" % CmpAnnualReportId)
                    logging.getLogger().info("%s" % (ReportBasicInfo['CmpName'] + key_desc[0:4]))

                    logging.getLogger().info("succ got year report: %s" % json.dumps(CmpAnnualReport, encoding='utf-8', ensure_ascii=False))
                    # logging.getLogger().info(res)

                else:
                    db.sql_update("UPDATE `gs_fujian_yearreport` SET `etl_status` = 6 WHERE `id` = %d" % int(uid))
                    logging.getLogger().info("uid : %s" % uid)
                    logging.getLogger().info("company_id : %s" % company_id)
                    logging.getLogger().info("this info already exists")
                    logging.getLogger().info("%s" % CmpAnnualReportId)
                    logging.getLogger().info(">>>>>>>> exeits %s" % (ReportBasicInfo['CmpName'] + key_desc[0:4]))


