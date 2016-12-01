# encoding=utf-8
import json

import GonggaoWebEsConvert

if __name__ == '__main__':
    old_data = u'''
    {
      "gonggaoren": "江苏省徐州市中级人民法院",
      "no": "5017857",
      "gonggaoshijian": "2016-10-26",
      "href": "/bulletin/court/2016/10/26/5017857.html",
      "file": "/psca/lgnot/bulletin/download/5017857.pdf",
      "type": "裁判文书",
      "dangshiren": "郭召琴、陆建明、徐州市金福源珠宝首饰有限公司"
    }
    '''
    web_data = u'''
      <div class="wrap1000 height568 bord">
        <div class="bgcf2 d1 ft1416">
            <p class="dots fl ml10 mt10"></p>
            <span class="pl5 c216cc1 fl mt8">当前位置 :&nbsp;</span>
            <span class="  pointer mt8 c216cc1 fl">公告内容</span>
            <p class="gt fl ml5 mt12 mr5"></p>
            <span class="fl c216cc1 mt8 pointer">裁判文书</span>
        </div>
        <div class="d2">
            <div class="d21">
                <p class="ft1616 fsb" >裁判文书</p>
                <p class="ft1416 fsb pt10">郭召琴、陆建明、徐州市金福源珠宝首饰有限公司</p>
            </div>
        <div class="d22 pt10">
        <p class="ft1424">
            郭召琴、陆建明、徐州市金福源珠宝首饰有限公司：本院受理任永萍诉你们民间借贷纠纷一案，已审理终结。现依法向你们公告送达（2015）徐商终字第1024号民事判决书。自公告之日起，60日内来本院领取民事判决书，逾期则视为送达。
        </p>
        </div>
        <div class="d23 fr ">
            <p class="ft1424 fsb">[江苏]江苏省徐州市中级人民法院</p>
            <p class="ft1424">刊登版面：七版</p>
            <p class="ft1424">刊登日期：2016-10-26</p>
            <p class="ft1424">上传日期：2016-10-26</p>
        </div>
        <div class="cl"></div>
        <div class="d24 tac">
            <a href="/psca/lgnot/bulletin/download/5017857.pdf" ><p class=" pdf mauto"></p></a>
            <p class="ft1416 fsb"><a href="/psca/lgnot/bulletin/download/5017857.pdf">下载打印本公告</a></p>
            <p class="ft1416">(公告样报已直接寄承办法官)</p>
        </div>
    </div><!--d2结束-->
    '''
    res = GonggaoWebEsConvert.convert(old_data, web_data)
    print json.dumps(res)
